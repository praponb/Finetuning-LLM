from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() & ((F.col("cost") < 72) | (F.col("status") == "LOST")))
    .select(
        "id",
        "shipped_date",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
    )
    .orderBy(F.col("shipped_date").desc())
)
