from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.customer_id").isNotNull())
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
        F.when(F.col("customers.country").isin(["DE", "US"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(F.countDistinct(F.col("subscriptions.monthly_fee")).alias("unique_customers"))
)
