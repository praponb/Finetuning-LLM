from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel") == "partner")
    .groupBy(
        "status",
        F.when(F.col("discount") >= 0.34, "high").when(F.col("discount") >= 0.06, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(
        F.count("amount").alias("amount_count"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("discount")).otherwise(0)).alias("mobile_discount"),
    )
    .filter(F.col("amount_count") > 2)
    .orderBy(F.col("mobile_discount").desc())
)
