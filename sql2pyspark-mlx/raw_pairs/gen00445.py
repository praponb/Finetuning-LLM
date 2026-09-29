from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("device") == "desktop", 1).otherwise(0)).alias("desktop_count"),
        F.max("started_at").alias("latest_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.segment"),
        F.col("c.name"),
        F.col("t.desktop_count"),
        F.col("t.latest_started_at"),
    )
    .orderBy("name")
)
