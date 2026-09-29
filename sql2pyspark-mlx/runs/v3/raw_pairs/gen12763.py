from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("channel") != "store") & (F.col("shipping_fee") > 14))
    .groupBy("status")
    .agg(
        F.count(F.when(F.col("status") == "CANCELLED", 1)).alias("cancelled_count"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
        F.sum(F.when(F.col("channel") == "web", F.col("discount")).otherwise(0)).alias("web_discount"),
    )
    .orderBy("cancelled_count", F.col("mobile_discount").desc(), "status")
    .limit(10)
)
