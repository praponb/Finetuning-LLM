from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.col("customers.credit_limit") >= 5400)
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("customers.signup_date"),
        F.col("customers.city"),
    )
    .orderBy("started_at")
)
