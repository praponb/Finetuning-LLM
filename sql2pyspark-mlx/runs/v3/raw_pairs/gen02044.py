from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("s.delivered_date"),
    )
    .orderBy("shipped_date")
)
