from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.credit_limit") >= 15600, "high").when(F.col("c.credit_limit") >= 3900, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.col("c.city"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("s.browser")))).alias("browser_values"),
        F.min(F.col("s.started_at")).alias("min_started_at"),
    )
)
