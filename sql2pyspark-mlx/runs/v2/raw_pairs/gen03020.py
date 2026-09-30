from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .groupBy(
        F.year(F.col("c.signup_date")).alias("signup_year"),
        F.month(F.col("c.signup_date")).alias("signup_month"),
    )
    .agg(F.countDistinct(F.col("s.monthly_fee")).alias("unique_customers"))
)
