from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.count(F.when(F.col("s.status") == "ACTIVE", 1)).alias("active_count"),
        F.round(((100 * F.sum(F.when(F.col("s.plan") == "standard", 1).otherwise(0))) / F.count("*")), 1).alias("pct_standard"),
    )
)
