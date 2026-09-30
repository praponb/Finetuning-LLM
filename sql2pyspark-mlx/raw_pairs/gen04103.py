from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .select(
        F.col("s.id"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("c.name"),
        (F.datediff(F.current_date(), F.col("s.started_at")) / 7).alias("weeks_since_started_at"),
    )
)
