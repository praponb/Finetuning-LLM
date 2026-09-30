from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 1520)
    .groupBy("channel", F.month("order_date").alias("order_month"))
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
    )
    .filter(F.col("avg_amount") > 74)
    .select("channel", "order_month", "avg_amount", "channel_values")
)
