from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(~F.col("customers.country").isin(["US", "TH", "SG"]))
    .select(
        F.col("sessions.device"),
        F.col("sessions.started_at"),
        F.col("sessions.browser"),
        F.col("customers.name"),
    )
    .orderBy("device")
)
