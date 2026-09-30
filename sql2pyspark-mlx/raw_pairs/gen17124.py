from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("order_id") == 13) & (F.col("cost") <= 85))
    .withColumn("status_lower", F.lower("status"))
)

agg = (
    valid_shipments
    .groupBy("carrier", "status")
    .agg(F.count("cost").alias("cost_count"))
)

result = (
    agg
    .select("carrier", "status", "cost_count")
    .orderBy(F.col("cost_count").desc())
)
