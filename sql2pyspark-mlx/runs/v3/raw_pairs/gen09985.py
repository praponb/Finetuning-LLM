from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.device"),
        F.col("customers.email"),
        F.concat_ws("/", F.col("customers.email"), F.col("customers.segment")).alias("email_segment_key"),
    )
)
