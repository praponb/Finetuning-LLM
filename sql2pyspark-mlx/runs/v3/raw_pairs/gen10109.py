from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("delivered_date") > "2025-06-01").alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .select(
        F.col("o.id"),
        F.col("o.discount"),
        F.col("o.status"),
        F.col("o.customer_id"),
    )
)
