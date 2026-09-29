from pyspark.sql import functions as F

recent_stores = (
    df_stores
    .filter((F.col("opened_date") <= "2024-01-27") & F.col("sqft").isNotNull() & (F.col("sqft") == 5975))
    .withColumn("city_group", F.when(F.col("city").isin(["Berlin", "Chiang Mai"]), "Group A").otherwise("Other"))
)

stores_stats = (
    recent_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.max("sqft").alias("max_sqft"))
)

result = (
    stores_stats
    .filter(F.col("max_sqft") < 6000)
    .select("opened_year", "max_sqft")
    .orderBy(F.col("max_sqft").desc(), F.col("opened_year").desc())
    .limit(3)
)
