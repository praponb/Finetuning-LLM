from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .filter(F.col("subscriptions.customer_id").isNotNull())
    .join(df_customers.filter(F.col("country").isin(["UK", "US", "SG"])).alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_anti")
    .select(F.col("subscriptions.id"), F.col("subscriptions.status"))
)
