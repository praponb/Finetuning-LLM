from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("country") == "SG", 1).otherwise(0)).alias("sg_count"),
        F.min("started_at").alias("min_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .filter(F.col("t.sg_count") >= 48)
    .select(
        F.col("c.name"),
        F.col("c.city"),
        F.col("t.sg_count"),
        F.col("t.min_started_at"),
    )
    .orderBy("min_started_at")
)
