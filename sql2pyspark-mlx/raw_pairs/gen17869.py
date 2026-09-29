from pyspark.sql import functions as F

recent_stores = df_stores.filter(F.col("opened_date") >= "2023-05-01")

region_summary = (
    recent_stores
    .groupBy("region")
    .agg(F.sum(F.when(F.col("region") == "East", 1).otherwise(0)).alias("east_count"))
)

result = (
    region_summary
    .filter(F.col("east_count") > 79)
    .orderBy("east_count", "region")
)
