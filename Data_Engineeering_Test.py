import sys
from pyspark.sql import SparkSession
sys.getdefaultencoding()

if __name__ == '__main__':

    ''' Spark Session Building Entry point & Configuration of xml and Postgresql jar to connect read and write '''

    spark = SparkSession.builder.appName("Data_Engineering_Test").master("local[*]")\
        .config("spark.jars", "file:///D://GAME//spark-xml_2.11-0.5.0.jar")\
        .config("spark.executor.extraClassPath", "file:///D://GAME//spark-xml_2.11-0.5.0.jar")\
        .config("spark.executor.extraLibrary", "file:///D://GAME//spark-xml_2.11-0.5.0.jar")\
        .config("spark.diver.extraClassPath", "file:///D://GAME//spark-xml_2.11-0.5.0.jar")\
        .config("spark.jars", "file:///D://GAME//postgresql-42.2.11.jar")\
        .config("spark.driver.extraClassPath", "file:///D://GAME//postgresql-42.2.11.ja")\
        .getOrCreate()


    spark.conf.set("spark.shuffle.service.enabled", "false")
    spark.conf.set("spark.dynamicAllocation.enable", "false")
    spark.conf.set("spark.executor.memory", '10g')
    spark.conf.set('spark.executor.cores', '4')
    spark.conf.set('spark.cores.max', '2')
    spark.conf.set("spark.driver.memory", '10g')

    ''' Reading Xml & Csv file  '''
    enwiki_latest_abstract = spark.read.format("xml").options(rowTag="feed").options(rowTag="doc").load("D:\\GAME\\enwiki-latest-abstract\\enwiki-latest-abstract.xml")
    enwiki_latest_abstract.printSchema()
    enwiki_latest_abstract.createOrReplaceTempView('abstract_file')
    wiki_file = spark.sql(""" select * from abstract_file """).cache()
    wiki_file.createOrReplaceTempView('wiki_f')
    '''  Applying Replace to getting only Movie file from title '''
    wiki_file_01 = spark.sql("""  select 
                    replace(title,"Wikipedia: ","") as title,
                    url,
                    links,
                    abstract
                    from wiki_f """).cache()
    wiki_file_01.createOrReplaceTempView('wiki')
    movie_df = spark.read.format("csv").option("inferSchema", "True").option("header", "True").option("quote", "\"").load("D:\\GAME\\movies_metadata.csv")
    rating_df = spark.read.format("csv").option("inferSchema","True").option("header", "True").load("D:\\GAME\\ratings\\ratings.csv")
    movie_df.printSchema()
    movie_df.createOrReplaceTempView("movie")
    rating_df.createOrReplaceTempView("rating")
    ''' Calculate Ratio of budget to revenue '''
    spark.sql(""" select  round(((avg(budget)-1)/(avg(revenue)-1)) * 100) as Ratio_budget_revenue from movie  group by id """).show(truncate=False)
    ''' Fetching Movie & Wikipedia url'''
    spark.sql("""    
    select 
    A.id,
    A.original_title as Movie_Original_title,
    A.title as Movie_title,
    B.title,
    B.url
    from 
    movie A inner join
    wiki B
    on A.title = B.title 
    """).show(truncate=False)

    ''' Loading Top 1000 Movie data with Wiki data on Ratio on postregsql Target Table'''

    trg = spark.sql("""
    select 
    A.title,
    A.budget,
    year(A.release_date) as Year,
    A.revenue,
    C.rating,
    round(((avg(A.budget)-1)/(avg(A.revenue)-1)) * 100) as Ratio,
    A.production_companies,
    B.url,
    B.abstract
    from 
    
    movie AS A 
    inner join wiki AS B on A.title = B.title
    inner join rating AS C on A.id = C.movieId
    
    group by A.title,
    A.budget,
    A.revenue,
    Year,
    C.rating,
    A.production_companies,
    B.url,
    B.abstract
    ORDER BY Ratio desc
    limit 1000   """).cache()

    print("correctness Test in Trg data")

    spark.sql(" select count(*) from movie from title in (select title from wiki)").show(truncate=False)

    trg.write \
        .option("createTableOptions", "Movie_Top_Rated") \
        .jdbc("jdbc:postgresql:dbserver", "schema.Movie_Top_Ratee",
              properties={"user": "username", "password": "password"})

spark.stop()



