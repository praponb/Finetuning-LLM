from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count(F.when(F.col("country") == "US", 1)).alias("us_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "left")
    .filter(F.col("cs.us_count") >= 10)
    .select(F.col("c.age"), F.col("c.is_active"), F.col("cs.us_count"))
    .orderBy(F.col("us_count").desc())
)
