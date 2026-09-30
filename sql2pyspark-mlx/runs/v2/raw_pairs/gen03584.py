from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .filter(F.col("s.region").isNotNull())
    .groupBy(F.col("s.city"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
)
