from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 35, "high").when(F.col("shipping_fee") >= 14, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
        F.round(F.stddev("amount"), 2).alias("stddev_amount"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
    .orderBy(F.col("mobile_discount").desc())
)
