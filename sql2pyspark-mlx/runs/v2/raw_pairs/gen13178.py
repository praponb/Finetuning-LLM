from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.filter(F.col("device") != "mobile").alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left_semi")
    .select(F.col("customers.id"), F.col("customers.is_active"))
)
