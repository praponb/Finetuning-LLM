from pyspark.sql import functions as F

orders_by_customer = (
    df_orders
    .filter(F.col("store_id").isNull())
    .groupBy("customer_id")
    .agg(
        F.round(F.avg("discount"), 2).alias("avg_discount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
)

result = (
    df_customers.alias("c")
    .join(orders_by_customer.alias("obc"), F.col("c.id") == F.col("obc.customer_id"), "left")
    .select(
        F.col("c.age"),
        F.col("c.segment"),
        F.col("obc.avg_discount"),
        F.col("obc.channel_values"),
    )
    .orderBy("segment")
)
