from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter((F.col("subscriptions.end_date") < "2024-11-26") & (F.col("subscriptions.end_date") <= "2023-08-01"))
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.status"),
        F.col("subscriptions.monthly_fee"),
        F.col("customers.is_active"),
        F.col("customers.age"),
        F.col("customers.city"),
        F.upper(F.col("customers.segment")).alias("segment_upper"),
    )
    .orderBy("city", "id")
    .limit(1)
)
