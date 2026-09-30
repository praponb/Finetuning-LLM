from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(F.col("customers.id"), F.col("customers.country"))
    .agg(
        F.count(F.col("sessions.id")).alias("session_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.device")))).alias("device_values"),
    )
    .orderBy(F.col("country").desc())
)
