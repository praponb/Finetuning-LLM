from pyspark.sql import functions as F

base = df_stores.filter(F.col("sqft") < 12376)

stores_stats = (
    base
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.max("sqft").alias("max_sqft"))
)

result = (
    stores_stats
    .filter(F.col("max_sqft") >= 13100)
    .select("opened_year", "max_sqft")
)
