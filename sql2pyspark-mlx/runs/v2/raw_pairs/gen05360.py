from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("customers.id") == F.col("sessions.customer_id"), "left_semi")
    .select(F.col("customers.id"), F.col("customers.city"))
    .orderBy("city")
)
