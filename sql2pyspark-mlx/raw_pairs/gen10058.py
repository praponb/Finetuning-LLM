from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("region").isNotNull())
    .withColumn("opened_date_dayofweek", F.dayofweek("opened_date"))
)

agg = (
    src
    .groupBy("city")
    .agg(F.min("sqft").alias("min_sqft"))
)

result = agg.select("city", "min_sqft")
