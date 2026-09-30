from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("sessions.customer_id") == 6)
    .select(
        F.col("sessions.browser"),
        F.col("sessions.country"),
        F.col("customers.email"),
    )
)
