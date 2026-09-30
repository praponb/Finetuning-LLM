from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "left")
    .filter(F.col("t.total_count") > 10)
    .select(F.col("c.segment"), F.col("c.city"), F.col("t.total_count"))
)
