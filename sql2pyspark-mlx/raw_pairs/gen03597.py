from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter((F.col("s.started_at") < "2023-09-01") & (F.col("c.signup_date") >= "2025-05-01"))
    .select(F.col("s.started_at"), F.col("s.customer_id"), F.col("c.signup_date"))
)
