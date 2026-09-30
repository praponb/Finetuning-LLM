from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        F.when(F.col("credit_limit") >= 12500, "high").when(F.col("credit_limit") >= 7900, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.min("credit_limit").alias("min_credit_limit"),
        F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0)).alias("chiang_mai_count"),
        F.countDistinct("credit_limit").alias("unique_citys"),
    )
    .filter(F.col("unique_citys") >= 1)
    .drop("unique_citys")
)
