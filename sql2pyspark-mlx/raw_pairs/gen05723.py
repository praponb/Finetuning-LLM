from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "inner")
    .select(
        "e.id",
        "e.order_id",
        "e.shipped_date",
        "e.delivered_date",
        "e.status",
        F.col("d.order_date"),
        F.col("d.status").alias("order_status"),
    )
)
