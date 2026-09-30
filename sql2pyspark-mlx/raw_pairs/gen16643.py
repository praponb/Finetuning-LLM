from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("customers.country"),
        F.col("customers.city"),
        F.col("customers.name"),
        F.concat_ws("-", F.col("customers.segment"), F.col("customers.email")).alias("segment_email_key"),
    )
)
