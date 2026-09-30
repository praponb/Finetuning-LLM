from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.is_active"), F.col("c.city"))
    .agg(F.min(F.col("s.start_date")).alias("first_start_date"))
)
