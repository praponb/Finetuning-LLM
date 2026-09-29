from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("subscriptions")
    .join(df_customers.alias("customers"), F.col("subscriptions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("subscriptions.monthly_fee") <= 200)
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(F.avg(F.col("subscriptions.monthly_fee")).alias("avg_monthly_fee"))
)
