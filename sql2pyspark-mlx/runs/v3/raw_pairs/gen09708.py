from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("browser").isin(["Firefox", "Safari", "Chrome"]))
    .groupBy("customer_id")
    .agg(F.max("started_at").alias("last_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.name"),
        F.col("cs.last_started_at"),
    )
)
