from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .filter(F.col("sessions.customer_id").isin([12, 37]) & (F.col("sessions.browser") == "Firefox"))
    .select(F.col("sessions.id"), F.col("sessions.country"), F.col("customers.email"))
)
