from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.credit_limit").between(7800, 10800))
    .groupBy(F.col("c.country"), F.col("c.segment"))
    .agg(F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"))
    .orderBy("country", F.col("min_monthly_fee").desc())
)
