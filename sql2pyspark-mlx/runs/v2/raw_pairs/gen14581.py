from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") != "New York") & ~F.col("is_active"))
    .groupBy(
        F.when(F.col("credit_limit") >= 17500, "high").when(F.col("credit_limit") >= 7500, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.count("age").alias("age_count"),
        F.round(((100 * F.sum(F.when(F.col("segment") == "Corporate", 1).otherwise(0))) / F.count("*")), 1).alias("pct_corporate"),
    )
    .orderBy(F.col("pct_corporate").desc(), "credit_limit_bucket")
    .limit(50)
)
