from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 20, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "channel",
    )
    .agg(
        F.countDistinct("shipping_fee").alias("unique_statuss"),
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
    )
    .filter(F.col("pct_mobile") >= 45)
)
