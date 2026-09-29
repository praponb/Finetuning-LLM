from pyspark.sql import functions as F

result = (
    df_shipments
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        "s.id",
        "s.order_id",
        "s.carrier",
        F.col("o.status").alias("order_status"),
        F.col("o.discount"),
        F.col("o.order_date"),
    )
)
