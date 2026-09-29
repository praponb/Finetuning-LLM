from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.year(F.col("c.signup_date")) == 2025) | (F.col("s.status") != "CANCELLED"))
    .select(F.col("s.id"), F.col("s.start_date"), F.col("c.segment"))
    .orderBy(F.col("id").desc())
)
