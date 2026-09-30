from pyspark.sql import functions as F

customer_summary = (
    df_sessions
    .groupBy("customer_id")
    .agg(
        F.min("started_at").alias("first_started_at"),
        F.count("*").alias("cnt"),
    )
)

result = (
    df_customers.alias("customers")
    .join(customer_summary.alias("customer_summary"), F.col("customers.id") == F.col("customer_summary.customer_id"), "left")
    .filter(F.col("customer_summary.cnt") >= 6)
    .select(
        F.col("customers.id"),
        F.col("customers.segment"),
        F.col("customer_summary.first_started_at"),
        F.col("customer_summary.cnt"),
    )
)
