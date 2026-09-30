from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .groupBy(F.col("o.status"))
    .agg(F.count("*").alias("row_count"))
    .orderBy("status", F.col("row_count").desc())
)
