from pyspark.sql import functions as F

valid_shipments = (
    df_shipments
    .filter((F.col("shipped_date") > "2024-05-01") & (F.month("shipped_date").isin([1, 5, 8]) | (F.col("carrier") == "FedEx")))
    .withColumn("cost_band", F.when(F.col("cost") >= 95, "High").when(F.col("cost") >= 44, "Medium").otherwise("Low"))
)

status_summary = (
    valid_shipments
    .groupBy("status")
    .agg(
        F.min("delivered_date").alias("first_delivered_date"),
        F.min("delivered_date").alias("min_delivered_date"),
    )
)

result = (
    status_summary
    .select("status", "min_delivered_date")
    .orderBy(F.col("min_delivered_date").desc())
)
