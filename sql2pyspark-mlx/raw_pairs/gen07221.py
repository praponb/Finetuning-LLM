from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.when(F.col("c.age") >= 61, "high").when(F.col("c.age") >= 40, "mid").otherwise("low").alias("age_bucket"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(F.countDistinct(F.col("s.monthly_fee")).alias("unique_statuss"))
    .orderBy(F.col("unique_statuss").desc())
)
