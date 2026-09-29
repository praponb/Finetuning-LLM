from pyspark.sql import functions as F

recent_customers = (
    df_customers
    .filter(F.col("credit_limit").isNull() & ~F.col("is_active"))
    .withColumn("is_active_flag", F.col("is_active").cast("int"))
)

customers_stats = (
    recent_customers
    .groupBy(
        F.when(F.col("age") >= 72, "high").when(F.col("age") >= 28, "mid").otherwise("low").alias("age_bucket"),
        F.when(F.col("credit_limit") >= 16300, "high").when(F.col("credit_limit") >= 3000, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(F.sum("is_active_flag").alias("total_is_active_flag"))
)

result = (
    customers_stats
    .select("age_bucket", "credit_limit_bucket", "total_is_active_flag")
    .orderBy(F.col("total_is_active_flag").desc())
)
