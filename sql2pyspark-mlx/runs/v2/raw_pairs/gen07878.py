from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "left")
    .filter(F.col("s.status").isin(["DELIVERED", "LOST"]))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("o.store_id"),
        F.col("o.order_date"),
    )
)
