from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "right")
    .filter(~F.col("c.segment").isin(["SMB", "Corporate"]))
    .select(F.col("s.started_at"), F.col("c.signup_date"), F.col("c.city"))
    .orderBy("signup_date")
)
