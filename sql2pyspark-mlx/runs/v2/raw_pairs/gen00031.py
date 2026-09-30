from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.segment") == "SMB")
    .select(F.col("s.monthly_fee"), F.col("s.customer_id"), F.col("c.country"))
    .orderBy(F.col("customer_id").desc())
)
