from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .groupBy(
        F.when(F.year(F.col("c.signup_date")) < 2024, "before_2024").when(F.year(F.col("c.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
    )
    .agg(
        F.count(F.col("s.monthly_fee")).alias("monthly_fee_count"),
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_count") >= 2)
    .orderBy(F.col("monthly_fee_range").desc())
)
