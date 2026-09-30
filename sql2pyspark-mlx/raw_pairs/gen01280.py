from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "outer")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.city"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.channel")))).alias("channel_values"),
    )
)
