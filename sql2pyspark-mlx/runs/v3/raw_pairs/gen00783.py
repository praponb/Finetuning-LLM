from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(~F.col("c.is_active"))
    .select(
        F.col("s.device"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("c.age"),
        F.col("c.segment"),
        F.substring(F.col("c.city"), 1, 3).alias("city_prefix"),
    )
)
