from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("c.segment"),
        F.col("c.country"),
        F.col("c.age"),
        F.upper(F.col("c.segment")).alias("segment_upper"),
    )
)
