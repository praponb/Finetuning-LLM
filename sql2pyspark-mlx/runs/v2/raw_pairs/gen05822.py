from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.segment") != "Consumer") & F.col("c.is_active"))
    .select(
        F.col("s.device"),
        F.col("c.country"),
        F.col("c.email"),
        F.col("c.is_active"),
        F.to_date(F.col("s.started_at")).alias("started_at_date"),
    )
)
