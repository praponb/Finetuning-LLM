from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("customers.id") == F.col("sessions.customer_id"), "inner")
    .filter(F.to_date(F.col("sessions.started_at")) >= "2023-10-09")
    .groupBy(F.col("customers.country"))
    .agg(F.count("*").alias("total_count"))
)
