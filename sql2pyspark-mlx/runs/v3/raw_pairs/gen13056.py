from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .groupBy(
        F.col("c.city"),
        F.date_format(F.col("c.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.sum(F.when(F.col("s.status") == "CANCELLED", 1).otherwise(0)).alias("cancelled_count"),
    )
)
