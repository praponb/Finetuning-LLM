from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.customer_id").isNull() & (F.col("s.shipped_date") < "2023-07-16"))
    .select(
        F.col("s.id"),
        F.col("s.delivered_date"),
        F.col("s.status"),
        F.col("o.amount"),
        F.col("o.channel"),
        F.col("o.order_date"),
    )
)
