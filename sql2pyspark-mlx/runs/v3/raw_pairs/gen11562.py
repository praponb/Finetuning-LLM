from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .select(
        F.col("customers.credit_limit"),
        F.col("customers.segment"),
        F.col("sessions.country"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.upper(F.col("customers.segment")).alias("segment_upper"),
    )
)
