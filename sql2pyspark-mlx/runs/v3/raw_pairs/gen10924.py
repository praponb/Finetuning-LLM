from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("sessions.id"),
        F.col("sessions.country"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.col("customers.city"),
        F.col("customers.credit_limit"),
        F.col("customers.segment"),
        F.substring(F.col("sessions.device"), 1, 2).alias("device_prefix"),
    )
)
