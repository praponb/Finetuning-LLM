from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .filter(F.year(F.col("s.end_date")) >= 2024)
    .groupBy(
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
        F.col("c.city"),
    )
    .agg(F.max(F.col("s.monthly_fee")).alias("max_monthly_fee"))
    .orderBy("signup_month", F.col("max_monthly_fee").desc(), "city")
    .limit(3)
)
