from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "right")
    .filter(F.col("subscriptions.plan").isin(["enterprise", "basic"]))
    .select(
        F.col("subscriptions.end_date"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.city"),
        F.col("customers.signup_date"),
        F.add_months(F.col("customers.signup_date"), 3).alias("signup_date_plus_months"),
    )
    .orderBy(F.col("end_date").desc())
)
