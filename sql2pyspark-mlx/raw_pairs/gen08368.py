from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.end_date") >= "2025-05-01")
    .groupBy(
        F.when(F.col("c.age") >= 68, "high").when(F.col("c.age") >= 30, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(F.sum(F.col("s.monthly_fee")).alias("total_monthly_fee"))
    .orderBy(F.col("total_monthly_fee").desc(), "age_bucket")
)
