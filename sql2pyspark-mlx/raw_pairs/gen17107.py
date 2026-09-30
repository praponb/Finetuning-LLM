from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"), F.col("c.segment"))
    .agg(F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"))
    .filter(F.col("max_monthly_fee") > 170)
)
