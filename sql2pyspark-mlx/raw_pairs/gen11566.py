from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "inner")
    .groupBy(
        F.month(F.col("customers.signup_date")).alias("signup_month"),
        F.col("customers.city"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.device")))).alias("device_values"),
    )
    .orderBy("device_values")
)
