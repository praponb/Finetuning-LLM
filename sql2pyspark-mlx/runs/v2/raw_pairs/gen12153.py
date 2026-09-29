from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.monthly_fee").between(210, 220))
    .select(
        F.col("s.status"),
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.col("c.name"),
        F.col("c.is_active"),
    )
    .orderBy("status")
)
