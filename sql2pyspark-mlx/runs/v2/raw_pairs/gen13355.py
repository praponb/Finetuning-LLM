from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "left")
    .select(
        F.col("c.segment"),
        F.col("c.is_active"),
        F.col("s.device"),
        F.col("s.customer_id"),
    )
)
