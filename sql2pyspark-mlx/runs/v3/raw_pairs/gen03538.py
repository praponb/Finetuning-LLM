from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1400, "high").when(F.col("amount") >= 750, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count("*").alias("total_count"))
)
