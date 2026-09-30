from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.city"))
    .agg(
        F.count(F.col("sessions.id")).alias("session_count"),
        F.countDistinct(F.col("sessions.browser")).alias("unique_browsers"),
    )
)
