from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("store_id") == 31) & (F.col("amount") <= 890) & (F.col("amount") <= 1080))
    .groupBy("status")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
        F.round(F.avg("discount"), 2).alias("avg_discount"),
    )
)
