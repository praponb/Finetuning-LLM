from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.country").isin(["SG", "US"]))
    .select(
        F.col("sessions.id"),
        F.col("sessions.customer_id"),
        F.col("sessions.device"),
        F.col("sessions.country"),
        F.col("customers.name"),
    )
    .orderBy("device")
)
