from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.order_date") < "2025-08-12")
    .select(
        F.col("s.status"),
        F.col("o.order_date"),
        F.col("o.customer_id"),
        F.col("o.status").alias("order_status"),
    )
    .orderBy("order_status")
)
