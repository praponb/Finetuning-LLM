from pyspark.sql import functions as F

result = (
    df_orders.alias("orders")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("orders.customer_id"), "inner")
    .groupBy(F.col("customers.country"), F.col("customers.is_active"))
    .agg(
        F.sort_array(F.collect_set(F.col("orders.status"))).alias("status_list"),
        F.sort_array(F.collect_set(F.col("orders.channel"))).alias("channel_list"),
    )
    .orderBy("country")
)
