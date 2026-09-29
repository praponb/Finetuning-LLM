from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "outer")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .groupBy(F.month(F.col("s.opened_date")).alias("opened_month"))
    .agg(F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"))
    .orderBy(F.col("opened_month").desc())
)
