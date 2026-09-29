from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.min(F.col("s.monthly_fee")).alias("min_monthly_fee"))
    .filter(F.col("min_monthly_fee") > 120)
    .orderBy(F.col("signup_quarter").desc())
)
