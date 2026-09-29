from pyspark.sql import functions as F

customer_stats = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-07-04", "2025-01-01"))
    .groupBy("customer_id")
    .agg(F.max("end_date").alias("latest_end_date"))
)

result = (
    df_customers.alias("c")
    .join(customer_stats.alias("t"), F.col("c.id") == F.col("t.customer_id"), "inner")
    .select(
        F.col("c.id"),
        F.col("c.credit_limit"),
        F.col("c.email"),
        F.col("t.latest_end_date"),
    )
    .orderBy("credit_limit", "id")
    .limit(10)
)
