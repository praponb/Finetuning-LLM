from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.col("customers.country") != "TH")
    .groupBy(F.col("customers.is_active"))
    .agg(
        F.count("*").alias("row_count"),
        F.max(F.col("sessions.started_at")).alias("last_started_at"),
    )
    .orderBy("last_started_at", F.col("row_count").desc())
)
