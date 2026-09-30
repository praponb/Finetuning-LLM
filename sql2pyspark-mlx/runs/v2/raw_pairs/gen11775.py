from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.plan") != "standard")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.monthly_fee"),
        F.col("s.end_date"),
        F.col("c.signup_date"),
        F.col("c.segment"),
        F.initcap(F.trim(F.col("c.city"))).alias("city_title"),
    )
)
