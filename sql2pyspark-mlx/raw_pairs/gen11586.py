from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .groupBy(F.month(F.col("c.signup_date")).alias("signup_month"), F.col("c.city"))
    .agg(
        F.count(F.when(F.col("s.status") == "ACTIVE", 1)).alias("active_count"),
        F.count("*").alias("num_subscriptions"),
    )
    .orderBy(F.col("active_count").desc())
)
