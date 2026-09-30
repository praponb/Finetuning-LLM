from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter(F.col("s.country") != "DE")
    .select(
        F.col("s.device"),
        F.col("s.country"),
        F.col("s.started_at"),
        F.col("c.segment"),
        F.col("c.credit_limit"),
    )
)
