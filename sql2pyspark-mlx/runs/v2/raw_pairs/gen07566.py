from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("s.id") == F.col("o.store_id"), "inner")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("s.store_name").like("Si%"))
    .groupBy(F.col("c.country"))
    .agg(
        F.round(F.stddev(F.col("o.discount")), 2).alias("stddev_discount"),
        F.sort_array(F.collect_set(F.col("o.channel"))).alias("channel_list"),
    )
)
