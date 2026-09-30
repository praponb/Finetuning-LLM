from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("device") == "desktop")
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.max("started_at").alias("latest_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(
        F.col("c.id"),
        F.col("c.city"),
        F.col("cs.us_count"),
        F.col("cs.latest_started_at"),
    )
    .orderBy("latest_started_at")
)
