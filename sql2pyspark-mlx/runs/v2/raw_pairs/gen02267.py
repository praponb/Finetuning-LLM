from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(~F.col("customers.is_active"))
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.status"),
        F.col("subscriptions.customer_id"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.is_active"),
        F.col("customers.signup_date"),
        F.col("customers.credit_limit"),
        F.concat(F.col("customers.email"), F.lit("/"), F.col("customers.segment")).alias("email_segment"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
