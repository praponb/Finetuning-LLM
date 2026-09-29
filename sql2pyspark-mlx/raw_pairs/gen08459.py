from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("c.country"),
        F.col("c.is_active"),
        F.to_date(F.col("s.started_at")).alias("started_at_date"),
    )
    .orderBy("country", "id")
    .limit(20)
)
