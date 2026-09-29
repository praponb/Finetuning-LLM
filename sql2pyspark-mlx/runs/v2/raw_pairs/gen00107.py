from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.plan") != "premium")
    .groupBy(F.col("customers.segment"))
    .agg(
        F.count("*").alias("num_subscriptions"),
        F.min(F.col("subscriptions.end_date")).alias("min_end_date"),
    )
    .orderBy("num_subscriptions", "min_end_date")
)
