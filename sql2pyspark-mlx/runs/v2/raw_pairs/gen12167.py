from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.city"))
    .agg(F.sum(F.when(F.col("o.channel") == "web", 1).otherwise(0)).alias("web_count"))
)
