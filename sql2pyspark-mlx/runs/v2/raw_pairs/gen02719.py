from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 20)
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.max("discount").alias("max_discount"),
    )
    .filter(F.col("max_discount") >= 0.12)
    .select("order_month", "channel_values", "max_discount")
)
