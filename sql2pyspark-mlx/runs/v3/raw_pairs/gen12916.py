from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1560, "high").when(F.col("amount") >= 430, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
    .filter(F.col("total_count") >= 8)
    .select("amount_bucket", "total_count")
    .orderBy("amount_bucket")
)
