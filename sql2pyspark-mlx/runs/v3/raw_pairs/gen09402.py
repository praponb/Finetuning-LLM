from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["COMPLETED", "RETURNED"]), "group_a").otherwise("other").alias("status_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0))) / F.count("*")), 1).alias("pct_mobile"),
        F.sum(F.when(F.col("channel") == "store", 1).otherwise(0)).alias("store_count"),
        F.sum("discount").alias("total_discount"),
    )
    .orderBy(F.col("status_group").desc())
    .limit(5)
)
