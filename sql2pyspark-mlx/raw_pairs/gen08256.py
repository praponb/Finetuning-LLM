from pyspark.sql import functions as F

sessions_by_customer = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("browser") == "Chrome", 1)).alias("chrome_count"))
)

result = (
    df_customers.alias("customers")
    .join(sessions_by_customer.alias("SessionsByCustomer"), F.col("customers.id") == F.col("SessionsByCustomer.customer_id"), "left")
    .filter(F.col("SessionsByCustomer.chrome_count") >= 8)
    .select(
        F.col("customers.id"),
        F.col("customers.signup_date"),
        F.col("SessionsByCustomer.chrome_count"),
    )
)
