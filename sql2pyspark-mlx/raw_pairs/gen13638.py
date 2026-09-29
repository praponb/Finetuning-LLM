from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.country"))
    .agg(F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") >= 90)
    .orderBy(F.col("max_monthly_fee").desc(), F.col("country").desc())
)
