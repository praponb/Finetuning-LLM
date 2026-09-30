from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .filter(F.col("customers.segment") == "SMB")
    .groupBy(F.col("customers.id"), F.col("customers.country"))
    .agg(F.count(F.col("sessions.id")).alias("session_count"))
    .filter(F.col("session_count") > 1)
)
