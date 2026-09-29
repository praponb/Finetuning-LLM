from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .filter(F.upper(F.col("customers.name")).like("%IM %"))
    .groupBy(
        F.date_trunc("MONTH", F.col("customers.signup_date")).alias("signup_month"),
    )
    .agg(F.max(F.col("sessions.started_at")).alias("latest_started_at"))
)
