from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy(
        "is_active",
        F.when(F.col("age") >= 70, "high").when(F.col("age") >= 38, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.sum("credit_limit").alias("total_credit_limit"),
        F.sum(F.when(F.col("country") == "JP", 1).otherwise(0)).alias("jp_count"),
    )
    .filter(F.col("jp_count") > 20)
    .drop("jp_count")
)
