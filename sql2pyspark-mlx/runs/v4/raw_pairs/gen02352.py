from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.filter(F.col("signup_date") <= "2023-11-01").alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_semi")
    .select(
        F.col("sessions.id"),
        F.col("sessions.browser"),
        F.col("sessions.customer_id"),
        F.col("sessions.started_at"),
    )
)
