from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.plan").isin(["standard", "premium"]) & (F.col("subscriptions.monthly_fee") > 180))
    .select(
        F.col("subscriptions.start_date"),
        F.col("customers.age"),
        F.col("customers.name"),
        F.col("customers.credit_limit"),
    )
)
