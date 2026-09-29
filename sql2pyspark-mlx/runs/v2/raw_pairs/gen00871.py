from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(
        F.quarter(F.col("c.signup_date")).alias("signup_quarter"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        (F.max(F.col("s.monthly_fee")) - F.min(F.col("s.monthly_fee"))).alias("monthly_fee_range"),
    )
    .filter(F.col("monthly_fee_range") > 73)
)
