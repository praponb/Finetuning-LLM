from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter(F.col("customer_id").isin([19, 35]))
    .withColumn("channel_status", F.concat("channel", F.lit("/"), "status"))
)

summary = (
    filtered_orders
    .groupBy("customer_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.min("order_date").alias("min_order_date"),
    )
)

result = summary
