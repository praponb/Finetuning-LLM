from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(~F.col("country").isin(["DE", "UK"]))
    .groupBy("customer_id")
    .agg(F.max("started_at").alias("latest_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.is_active"), F.col("cs.latest_started_at"))
    .orderBy("latest_started_at")
)
