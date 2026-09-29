from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.start_date") >= "2024-04-01")
    .select(F.col("s.status"), F.col("c.email"))
    .orderBy("status")
)
