from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "DELIVERED")
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
    )
    .orderBy(F.col("cost_filled").desc())
)
