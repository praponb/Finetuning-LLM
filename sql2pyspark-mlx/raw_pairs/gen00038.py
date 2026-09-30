from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"), F.col("s.city"))
    .agg(F.count(F.when(F.col("o.status") == "SHIPPED", 1)).alias("shipped_count"))
)
