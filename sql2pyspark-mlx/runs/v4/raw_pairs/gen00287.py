from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(F.col("c.age") < 51)
    .groupBy(F.year(F.col("c.signup_date")).alias("signup_year"))
    .agg(F.max(F.col("s.started_at")).alias("last_started_at"))
    .orderBy("last_started_at")
)
