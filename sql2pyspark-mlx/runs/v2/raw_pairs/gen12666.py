from pyspark.sql import functions as F

customer_totals = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.sum(F.when(F.col("country") == "DE", 1).otherwise(0)).alias("de_count"),
        F.max("started_at").alias("last_started_at"),
    )
)

result = (
    df_customers.alias("c")
    .join(customer_totals.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(F.col("c.city"), F.col("t.de_count"), F.col("t.last_started_at"))
    .orderBy(F.col("de_count").desc())
)
