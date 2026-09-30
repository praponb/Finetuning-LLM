from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("country").isNotNull())
    .groupBy("customer_id")
    .agg(F.max("started_at").alias("latest_started_at"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("customer_stats"), F.col("customers.id") == F.col("customer_stats.customer_id"), "inner")
    .select(F.col("customers.name"), F.col("customer_stats.latest_started_at"))
)
