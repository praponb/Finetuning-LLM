from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.filter(F.col("is_active")).alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("sessions.country") != "SG")
    .select(F.col("sessions.id"), F.col("sessions.device"))
)
