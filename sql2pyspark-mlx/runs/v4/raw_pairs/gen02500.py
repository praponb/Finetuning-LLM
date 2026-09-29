from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("subscriptions.customer_id"), "inner")
    .filter(F.col("subscriptions.start_date").between("2023-04-27", "2025-04-01"))
    .groupBy(F.col("customers.city"), F.col("customers.is_active"))
    .agg(
        F.max(F.col("subscriptions.start_date")).alias("last_start_date"),
        F.min(F.col("subscriptions.start_date")).alias("min_start_date"),
    )
    .orderBy("last_start_date", "city", "is_active")
    .limit(5)
)
