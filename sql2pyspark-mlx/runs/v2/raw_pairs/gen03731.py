from pyspark.sql import functions as F

result = (
    df_stores.alias("s")
    .join(df_orders.alias("o"), F.col("s.id") == F.col("o.store_id"), "left")
    .groupBy(F.col("s.id"))
    .agg(F.count(F.col("o.id")).alias("order_count"))
    .orderBy("id")
    .limit(3)
)
