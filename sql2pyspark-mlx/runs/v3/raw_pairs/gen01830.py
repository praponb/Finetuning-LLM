from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.quarter(F.col("customers.signup_date")).alias("signup_quarter"))
    .agg(
        F.max(F.col("sessions.started_at")).alias("last_started_at"),
        F.sum(F.when(F.col("sessions.device") == "mobile", 1).otherwise(0)).alias("mobile_count"),
    )
    .orderBy(F.col("signup_quarter").desc())
)
