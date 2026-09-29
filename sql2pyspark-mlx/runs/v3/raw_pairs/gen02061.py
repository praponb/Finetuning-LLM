from pyspark.sql import functions as F

customer_stats = (
    df_sessions
    .groupBy("customer_id")
    .agg(F.count("*").alias("row_count"))
)

result = (
    df_customers.alias("customers")
    .join(customer_stats.alias("t"), F.col("customers.id") == F.col("t.customer_id"), "left")
    .select(
        F.col("customers.signup_date"),
        F.col("customers.segment"),
        F.col("t.row_count"),
    )
)
