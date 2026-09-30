from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.monthly_fee") >= 80)
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.plan"),
        F.col("customers.name"),
        F.col("customers.signup_date"),
    )
)
