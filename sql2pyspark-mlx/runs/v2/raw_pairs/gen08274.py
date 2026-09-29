from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([13, 22, 31, 39]) & (F.col("customer_id") == 13))
    .groupBy("status", F.month("order_date").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .orderBy("status", "channel_values")
)
