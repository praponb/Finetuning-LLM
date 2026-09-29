from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([5, 30, 40]))
    .groupBy(
        F.when(F.col("amount") >= 1310, "high").when(F.col("amount") >= 780, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        F.count("discount").alias("discount_count"),
        F.sum("discount").alias("total_discount"),
    )
    .filter(F.col("total_discount") < 1.9)
)
