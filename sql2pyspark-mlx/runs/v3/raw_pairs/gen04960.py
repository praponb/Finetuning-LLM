from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 17400, "high").when(F.col("c.credit_limit") >= 6100, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.country"),
    )
    .agg(
        F.sum(F.when(F.col("s.browser") == "Chrome", 1).otherwise(0)).alias("chrome_count"),
    )
    .filter(F.col("chrome_count") > 70)
)
