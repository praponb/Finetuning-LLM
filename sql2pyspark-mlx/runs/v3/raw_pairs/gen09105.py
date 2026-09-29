from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("delivered_date") <= "2025-04-23") & (F.col("shipped_date") < "2023-07-01"))
    .withColumn("days_shipped_date_to_delivered_date", F.datediff("delivered_date", "shipped_date"))
)

shipments_stats = (
    base
    .groupBy("status", "carrier")
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", 1).otherwise(0)).alias("flash_count"),
        (F.max("days_shipped_date_to_delivered_date") - F.min("days_shipped_date_to_delivered_date")).alias("days_shipped_date_to_delivered_date_range"),
    )
)

result = (
    shipments_stats
    .select("status", "carrier", "days_shipped_date_to_delivered_date_range")
    .orderBy(F.col("status").desc())
)
