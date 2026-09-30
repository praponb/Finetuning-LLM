from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"), F.col("c.city"))
    .agg(
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.min(F.col("s.start_date")).alias("first_start_date"),
    )
    .filter(F.col("avg_monthly_fee") > 60)
    .orderBy(F.col("first_start_date").desc(), "signup_quarter", "city")
    .limit(5)
)
