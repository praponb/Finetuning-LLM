from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("discount") < 0.4) & (F.col("channel") == "partner"))
    .groupBy(
        F.month("order_date").alias("order_month"),
        F.when(F.col("shipping_fee") >= 44, "high").when(F.col("shipping_fee") >= 17, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.round(F.stddev("shipping_fee"), 2).alias("stddev_shipping_fee"),
        F.avg("shipping_fee").alias("avg_shipping_fee"),
    )
)
