from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.name").like("Kan%"))
    .groupBy(F.year(F.col("customers.signup_date")).alias("signup_year"))
    .agg(
        F.count(F.when(F.col("subscriptions.plan") == "enterprise", 1)).alias("enterprise_count"),
    )
)
