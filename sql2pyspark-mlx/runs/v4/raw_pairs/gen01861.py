from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.region"))
    .agg(F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"))
    .orderBy(F.col("region").desc())
)
