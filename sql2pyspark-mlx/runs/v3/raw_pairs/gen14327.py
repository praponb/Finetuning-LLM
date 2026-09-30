from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("browser") == "Firefox", 1).otherwise(0)).alias("firefox_count"),
        F.min("started_at").alias("min_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.firefox_count") >= 59)
    .select(F.col("c.name"), F.col("cs.firefox_count"), F.col("cs.min_started_at"))
    .orderBy(F.col("name").desc(), F.col("firefox_count").desc())
)
