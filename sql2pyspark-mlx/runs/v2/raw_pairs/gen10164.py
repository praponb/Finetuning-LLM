from pyspark.sql import functions as F

result = (
    df_subscriptions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "left")
    .filter(F.col("c.city").isin(["Chiang Mai", "Singapore", "Bangkok"]) & F.col("s.end_date").between("2024-12-01", "2025-09-29"))
    .select(
        F.col("s.id"),
        F.col("s.start_date"),
        F.col("c.is_active"),
        F.col("c.segment"),
        F.col("c.signup_date"),
    )
)
