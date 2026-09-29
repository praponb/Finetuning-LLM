from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id") == 36)
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "partner", 1).otherwise(0))) / F.count("*")), 1).alias("pct_partner"),
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
    )
    .orderBy(F.col("avg_shipping_fee").desc(), F.col("channel_values").desc())
)
