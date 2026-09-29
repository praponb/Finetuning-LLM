from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.min("started_at").alias("min_started_at"))
)

result = (
    df_customers.alias("c")
    .join(customer_summary.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.name"),
        F.col("c.signup_date"),
        F.col("t.min_started_at"),
    )
    .orderBy("id")
    .limit(5)
)
