from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .withColumn("cost_band", F.when(F.col("cost") >= 97, "High").when(F.col("cost") >= 46, "Medium").otherwise("Low"))
)

result = (
    cleaned
    .groupBy("carrier")
    .pivot("status", ["LOST", "IN_TRANSIT"])
    .agg(F.avg("cost"))
)
