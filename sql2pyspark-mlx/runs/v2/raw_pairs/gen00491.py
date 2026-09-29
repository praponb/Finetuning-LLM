from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_anti")
    .filter(F.col("customers.signup_date").between("2023-11-01", "2024-10-01"))
    .select(
        F.col("customers.id"),
        F.col("customers.credit_limit"),
        F.col("customers.city"),
        F.col("customers.signup_date"),
    )
)
