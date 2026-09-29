from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("customer_id").isin([11, 17]))
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("session_count"),
        F.sum(F.when(F.col("browser") == "Chrome", 1).otherwise(0)).alias("chrome_count"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("cs"), F.col("c.id") == F.col("cs.customer_id"), "inner")
    .select(
        F.col("c.name"),
        F.col("c.country"),
        F.col("cs.session_count"),
        F.col("cs.chrome_count"),
    )
    .orderBy("chrome_count")
)
