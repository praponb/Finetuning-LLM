from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_semi")
    .filter(F.col("customers.age") != 50)
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("customers.country"),
    )
)
