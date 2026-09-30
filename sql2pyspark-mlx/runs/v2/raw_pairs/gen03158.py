from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.filter(F.col("credit_limit") >= 13700).alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_semi")
    .select(
        F.col("sessions.id"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
        F.col("sessions.device"),
    )
)
