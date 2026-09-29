from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.round(F.avg(F.col("s.monthly_fee")), 2).alias("avg_monthly_fee"),
        F.count(F.when(F.col("s.status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .orderBy(F.col("expired_count").desc(), "signup_year")
    .limit(10)
)
