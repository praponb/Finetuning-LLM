from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("Mi%"))
    .groupBy(
        F.when(F.col("c.age") >= 62, "high").when(F.col("c.age") >= 30, "mid").otherwise("low").alias("age_bucket"),
        F.when(F.col("c.credit_limit") >= 13800, "high").when(F.col("c.credit_limit") >= 2800, "mid").otherwise("low").alias("credit_limit_bucket"),
    )
    .agg(
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
        F.sort_array(F.collect_set(F.col("s.status"))).alias("status_list"),
    )
)
