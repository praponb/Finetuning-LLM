from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "outer")
    .filter(F.col("s.end_date").between("2023-10-01", "2024-06-01"))
    .select(
        F.col("s.start_date"),
        F.col("s.end_date"),
        F.col("s.customer_id"),
        F.col("c.is_active"),
        F.col("c.age"),
        F.col("c.country"),
    )
    .orderBy(F.col("customer_id").desc())
)
