from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.col("c.age") >= 72, "high").when(F.col("c.age") >= 39, "mid").otherwise("low").alias("age_bucket"),
    )
    .agg(
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_range") >= 63)
    .orderBy(F.col("age_bucket").desc())
)
