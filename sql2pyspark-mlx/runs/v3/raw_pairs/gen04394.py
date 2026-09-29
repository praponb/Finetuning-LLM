from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(
        F.sum(F.when(F.col("s.status") == "EXPIRED", 1).otherwise(0)).alias("expired_count"),
    )
    .orderBy("signup_quarter")
)
