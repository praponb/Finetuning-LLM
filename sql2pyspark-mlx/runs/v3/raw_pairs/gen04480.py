from pyspark.sql import functions as F

valid_stores = df_stores.filter(F.col("sqft") == 15721)

region_summary = (
    valid_stores
    .groupBy("region", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.count(F.when(F.col("region") == "North", 1)).alias("north_count"))
)

result = (
    region_summary
    .select("region", "opened_month", "north_count")
    .orderBy(F.col("north_count").desc())
)
