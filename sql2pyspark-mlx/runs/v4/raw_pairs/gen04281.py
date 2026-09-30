from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNull())
    .groupBy(
        F.when(F.col("discount") >= 0.43, "high").when(F.col("discount") >= 0.2, "mid").otherwise("low").alias("discount_bucket"),
        "status",
    )
    .agg(
        F.avg("amount").alias("avg_amount"),
        F.count(F.when(F.col("channel") == "store", 1)).alias("store_count"),
    )
    .filter(F.col("store_count") < 9)
    .orderBy(F.col("avg_amount").desc())
)
