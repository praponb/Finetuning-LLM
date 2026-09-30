from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "right")
    .groupBy(F.col("s.city"))
    .agg(F.count(F.when(F.col("p.status") == "REFUNDED", 1)).alias("refunded_count"))
)
