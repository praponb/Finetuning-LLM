from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("customers")
    .join(sessions_by_customer.alias("sessions_by_customer"), F.col("customers.id") == F.col("sessions_by_customer.customer_id"), "inner")
    .filter(F.col("sessions_by_customer.desktop_count") > 43)
    .select(
        F.col("customers.is_active"),
        F.col("customers.email"),
        F.col("sessions_by_customer.desktop_count"),
        F.col("sessions_by_customer.first_started_at"),
    )
    .orderBy(F.col("email").desc())
)
