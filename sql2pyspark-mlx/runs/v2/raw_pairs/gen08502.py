from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy(F.col("order_count").desc(), "id", "city")
    .limit(20)
)
