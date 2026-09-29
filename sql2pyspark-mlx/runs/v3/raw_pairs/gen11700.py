from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 62, "high").when(F.col("c.age") >= 35, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        F.count(F.when(F.col("we.page") == "/product", 1)).alias("product_count"),
        F.countDistinct(F.col("we.duration_sec")).alias("unique_pages"),
    )
    .filter(F.col("product_count") >= 3)
    .orderBy(F.col("unique_pages").desc(), "age_bucket")
    .limit(5)
)
