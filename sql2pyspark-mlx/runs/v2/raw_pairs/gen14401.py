from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.filter(F.col("country") == "US").alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left_semi")
    .filter(F.col("sessions.customer_id").isNull())
    .select(F.col("sessions.id"), F.col("sessions.country"), F.col("sessions.device"))
)
