from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.shipped_date"),
        F.col("o.store_id"),
        F.col("o.status"),
    )
    .orderBy("delivered_date", "store_id")
)
