from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.max("started_at").alias("latest_started_at"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
    )
)

result = (
    df_customers.alias("customers")
    .join(sessions_by_customer.alias("sessions_by_customer"), F.col("customers.id") == F.col("sessions_by_customer.customer_id"), "inner")
    .select(
        F.col("customers.id"),
        F.col("customers.city"),
        F.col("customers.segment"),
        F.col("sessions_by_customer.latest_started_at"),
        F.col("sessions_by_customer.country_list"),
    )
)
