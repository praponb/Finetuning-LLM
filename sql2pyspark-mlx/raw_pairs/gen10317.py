from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .filter(F.col("customer_id").isNull())
    .groupBy("customer_id")
    .agg(F.count("*").alias("total_count"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.is_active"),
        F.col("c.country"),
        F.col("t.total_count"),
    )
    .orderBy(F.col("total_count").desc(), "id")
    .limit(10)
)
