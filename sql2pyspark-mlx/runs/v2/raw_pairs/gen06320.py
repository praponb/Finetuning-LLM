from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("c.id") == F.col("s.customer_id"), "inner")
    .filter((F.to_date(F.col("s.started_at")) < "2025-10-02") & F.col("we.page").isin(["/checkout", "/product"]))
    .select(
        F.col("we.session_id"),
        F.col("s.country"),
        F.col("c.email"),
        F.col("c.city"),
    )
    .orderBy(F.col("city").desc())
)
