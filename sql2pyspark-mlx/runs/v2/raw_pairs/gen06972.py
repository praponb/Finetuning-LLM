from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("name").like("bo%")).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("subscriptions.id"), F.col("subscriptions.monthly_fee"))
)
