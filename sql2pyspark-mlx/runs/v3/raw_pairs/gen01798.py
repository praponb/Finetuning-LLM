from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "outer")
    .select(
        F.col("s.browser"),
        F.col("s.country"),
        F.col("c.segment"),
        F.lower(F.col("s.country")).alias("country_lower"),
    )
    .orderBy("segment", "browser")
)
