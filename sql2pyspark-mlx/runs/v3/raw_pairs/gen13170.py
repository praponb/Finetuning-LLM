from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.when(F.col("c.country").isin(["US", "JP"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(F.min(F.col("s.end_date")).alias("first_end_date"))
)
