from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "outer")
    .groupBy(
        F.date_format(F.col("customers.signup_date"), "yyyy-MM").alias("signup_month"),
    )
    .agg(
        F.sort_array(F.collect_set(F.col("sessions.device"))).alias("device_list"),
        F.sort_array(F.collect_set(F.col("sessions.country"))).alias("country_list"),
    )
    .orderBy(F.col("signup_month").desc())
)
