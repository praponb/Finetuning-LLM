from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .select(
        F.col("p.id"),
        F.col("p.amount"),
        F.col("p.status"),
        F.col("o.status").alias("order_status"),
        F.col("o.store_id"),
        F.col("s.sqft"),
    )
    .orderBy("order_status")
)
