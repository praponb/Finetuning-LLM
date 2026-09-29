from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isNull() | (F.col("cost") < 79))
    .select(
        "id",
        "carrier",
        "cost",
        "order_id",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date"),
        F.round("cost", 1).alias("cost_rounded"),
    )
)
