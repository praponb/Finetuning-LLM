from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .select(
        F.col("sessions.country"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.col("customers.credit_limit"),
        F.lower(F.col("sessions.country")).alias("country_lower"),
    )
)
