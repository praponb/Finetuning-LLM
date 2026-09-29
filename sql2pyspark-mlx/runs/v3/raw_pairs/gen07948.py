from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.email").like("%@mail.co.th"))
    .select(
        F.col("subscriptions.id"),
        F.col("subscriptions.end_date"),
        F.col("subscriptions.status"),
        F.col("customers.city"),
        F.col("customers.name"),
        F.col("customers.email"),
    )
    .orderBy(F.col("name").desc())
)
