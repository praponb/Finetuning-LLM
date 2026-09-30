from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "left")
    .filter(F.col("customer_stats.safari_count") > 7)
    .select(
        F.col("customers.name"),
        F.col("customers.signup_date"),
        F.col("customer_stats.safari_count"),
    )
    .orderBy(F.col("name").desc())
)
