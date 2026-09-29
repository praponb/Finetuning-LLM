from pyspark.sql import functions as F

valid_stores = df_stores.filter((F.col("city") == "London") & F.col("sqft").between(5737, 9243))

summary = (
    valid_stores
    .groupBy("region", "city")
    .agg(F.count(F.when(F.col("region") == "West", 1)).alias("west_count"))
)

result = (
    summary
    .filter(F.col("west_count") > 5)
    .orderBy(F.col("west_count").desc(), "city")
)
