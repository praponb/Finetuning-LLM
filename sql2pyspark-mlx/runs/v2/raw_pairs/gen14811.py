from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.country").isin(["US", "JP"]) | (F.col("c.country") == "DE"))
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("s.browser"),
        F.col("c.segment"),
        F.col("c.is_active"),
    )
)
