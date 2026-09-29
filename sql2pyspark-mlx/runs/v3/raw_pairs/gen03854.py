from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.filter(F.col("segment") != "SMB").alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("subscriptions.id"), F.col("subscriptions.monthly_fee"))
    .orderBy(F.col("monthly_fee").desc(), "id")
    .limit(5)
)
