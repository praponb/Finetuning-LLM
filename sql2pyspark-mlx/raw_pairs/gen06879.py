from pyspark.sql import functions as F

result = (
    df_orders
    .filter(~F.col("channel").isin(["mobile", "partner"]) & F.col("discount").isNotNull())
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.min("order_date").alias("min_order_date"),
    )
    .orderBy("channel_values")
)
