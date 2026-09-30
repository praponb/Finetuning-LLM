from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_subscriptions.alias("subscriptions"), F.col("customers.id") == F.col("subscriptions.customer_id"), "left_semi")
    .filter(~F.col("customers.is_active"))
    .select(F.col("customers.id"), F.col("customers.email"))
)
