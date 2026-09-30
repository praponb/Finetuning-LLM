from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .filter(F.col("device").isin(["desktop", "tablet"]))
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("browser") == "Safari", 1)).alias("safari_count"),
        F.min("started_at").alias("first_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.name"), F.col("cs.safari_count"), F.col("cs.first_started_at"))
    .orderBy(F.col("name").desc())
)
