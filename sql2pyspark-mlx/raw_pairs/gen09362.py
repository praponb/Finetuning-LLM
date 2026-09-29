from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("c.age") >= 35)
    .groupBy(F.quarter(F.col("c.signup_date")).alias("signup_quarter"))
    .agg(F.count("*").alias("session_count"))
    .orderBy(F.col("session_count").desc(), F.col("signup_quarter").desc())
)
