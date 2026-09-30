from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment") == "Consumer")
    .groupBy(F.col("customers.id"), F.col("customers.email"))
    .agg(
        F.count(F.col("sessions.id")).alias("session_count"),
        F.min(F.col("sessions.started_at")).alias("min_started_at"),
    )
    .orderBy("min_started_at")
)
