from pyspark.sql import functions as F

result = (
    df_sessions.alias("sessions")
    .join(df_customers.alias("customers"), F.col("sessions.customer_id") == F.col("customers.id"), "left")
    .groupBy(F.month(F.col("customers.signup_date")).alias("signup_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("sessions.device")))).alias("device_values"),
        F.countDistinct(F.col("sessions.browser")).alias("unique_browsers"),
    )
    .orderBy(F.col("unique_browsers").desc(), "signup_month")
    .limit(20)
)
