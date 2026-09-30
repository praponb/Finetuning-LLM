from pyspark.sql import functions as F

filtered_shipments = (
    df_shipments
    .filter((F.col("shipped_date") > "2025-01-01") | (F.col("status") == "IN_TRANSIT"))
    .withColumn("is_ups", F.when(F.col("carrier") == "UPS", 1).otherwise(0))
)

status_summary = (
    filtered_shipments
    .groupBy(
        "status",
        F.when(F.col("cost") >= 73, "high").when(F.col("cost") >= 37, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.round(F.avg("is_ups"), 2).alias("avg_is_ups"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_count"),
    )
)

result = (
    status_summary
    .filter(F.col("dhl_count") >= 8)
    .select("status", "cost_bucket", "avg_is_ups", "dhl_count")
    .orderBy("status", F.col("avg_is_ups").desc())
)
