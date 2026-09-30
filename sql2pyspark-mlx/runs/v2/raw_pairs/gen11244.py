from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.signup_date").between("2025-06-01", "2025-09-28") & F.col("customers.city").isin(["New York", "Berlin", "Bangkok"]))
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.plan"),
        F.col("customers.credit_limit"),
        F.col("customers.is_active").cast("int").alias("is_active_flag"),
    )
)
