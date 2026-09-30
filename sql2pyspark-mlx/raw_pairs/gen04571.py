from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_sessions.alias("s"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.device").isin(["mobile", "desktop"]))
    .select(
        F.col("c.city"),
        F.col("c.is_active"),
        F.col("c.name"),
        F.col("s.browser"),
        F.col("s.country"),
        F.col("s.device"),
    )
)
