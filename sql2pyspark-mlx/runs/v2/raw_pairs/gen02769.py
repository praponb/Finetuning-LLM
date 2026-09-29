from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_customers.alias("c"), F.col("o.customer_id") == F.col("c.id"), "left")
    .join(df_stores.alias("s"), F.col("o.store_id") == F.col("s.id"), "inner")
    .groupBy(F.col("s.region"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("o.channel")))).alias("channel_values"),
        F.max(F.col("o.order_date")).alias("latest_order_date"),
    )
    .orderBy("channel_values")
)
