from pyspark.sql import functions as F

result = (
    df_customers.alias("customers")
    .join(df_sessions.alias("sessions"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.col("customers.id"), F.col("customers.city"))
    .agg(
        F.count(F.col("sessions.id")).alias("session_count"),
        F.sort_array(F.collect_set(F.col("sessions.country"))).alias("country_list"),
    )
)
