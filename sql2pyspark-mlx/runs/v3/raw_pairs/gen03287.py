from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.when(F.year(F.col("customers.signup_date")) < 2024, "before_2024").when(F.year(F.col("customers.signup_date")) == 2024, "2024").otherwise("after_2024").alias("signup_cohort"),
        F.when(F.col("customers.country").isin(["JP", "UK"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(
        F.count(F.when(F.col("subscriptions.status") == "ACTIVE", 1)).alias("active_count"),
        (F.max(F.col("subscriptions.monthly_fee")) - F.min(F.col("subscriptions.monthly_fee"))).alias("monthly_fee_range"),
    )
    .orderBy("signup_cohort")
)
