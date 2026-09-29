from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.status"),
        F.col("s.order_id"),
        F.col("s.carrier"),
        F.col("o.amount"),
        F.col("o.order_date"),
    )
)
