from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.filter(F.col("country") == "SG").alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_semi")
    .select(F.col("sessions.id"), F.col("sessions.device"))
)
