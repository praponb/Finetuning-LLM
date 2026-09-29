from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter((F.year(F.col("s.end_date")) == 2025) & F.col("s.customer_id").isNotNull())
    .select(F.col("s.plan"), F.col("s.customer_id"), F.col("c.segment"))
    .orderBy("segment")
)
