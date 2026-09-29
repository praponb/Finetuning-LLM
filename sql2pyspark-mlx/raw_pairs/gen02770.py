from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("city") == "Singapore") | (F.col("signup_date") <= "2025-01-20"))
    .groupBy(
        F.when(F.col("credit_limit") >= 13800, "high").when(F.col("credit_limit") >= 3900, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.sort_array(F.collect_set("segment")).alias("segment_list"),
        F.round(((100 * F.sum(F.when(F.col("country") == "SG", 1).otherwise(0))) / F.count("*")), 1).alias("pct_sg"),
    )
)
