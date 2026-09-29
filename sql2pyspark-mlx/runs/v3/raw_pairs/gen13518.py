from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.monthly_fee") <= 170)
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .orderBy(F.col("signup_quarter").desc())
)
