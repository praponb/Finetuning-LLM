from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.started_at"),
        F.col("customers.segment"),
        F.col("customers.name"),
        F.col("customers.age"),
    )
)
