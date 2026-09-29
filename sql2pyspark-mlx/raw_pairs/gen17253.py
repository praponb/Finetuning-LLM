from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment"), F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(
        F.max(F.col("s.started_at")).alias("last_started_at"),
        F.count("*").alias("num_sessions"),
    )
    .orderBy(F.col("num_sessions").desc(), F.col("signup_year").desc())
)
