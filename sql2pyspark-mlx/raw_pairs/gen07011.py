from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .groupBy(
        F.month(F.col("c.signup_date")).alias("signup_month"),
        F.col("c.is_active"),
    )
    .agg(F.count("*").alias("cnt"))
    .orderBy(F.col("signup_month").desc(), "is_active")
    .limit(3)
)
