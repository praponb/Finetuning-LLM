from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("customer_id") == 21) & F.col("customer_id").isNull() & (F.col("store_id") == 12))
    .groupBy(
        F.when(F.col("discount") >= 0.44, "high").when(F.col("discount") >= 0.16, "mid").otherwise("low").alias("discount_bucket"),
    )
    .agg(F.avg("discount").alias("avg_discount"))
    .orderBy("avg_discount")
)
