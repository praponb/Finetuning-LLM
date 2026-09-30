from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("browser") == "Firefox", 1).otherwise(0)).alias("firefox_count"),
    )
)

result = (
    df_customers.alias("customers")
    .join(sessions_by_customer.alias("sessions_by_customer"), F.col("customers.id") == F.col("sessions_by_customer.customer_id"), "left")
    .select(
        F.col("customers.id"),
        F.col("customers.credit_limit"),
        F.col("sessions_by_customer.firefox_count"),
    )
    .orderBy(F.col("id").desc())
)
