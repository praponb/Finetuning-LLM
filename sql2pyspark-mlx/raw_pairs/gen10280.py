from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("min_started_at"),
        F.count("*").alias("num_sessions"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .select(F.col("c.country"), F.col("cs.min_started_at"), F.col("cs.num_sessions"))
)
