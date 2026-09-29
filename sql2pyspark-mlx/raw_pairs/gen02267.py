from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(F.col("c.country"), F.col("cs.min_started_at"))
    .orderBy("min_started_at", F.col("country").desc())
)
