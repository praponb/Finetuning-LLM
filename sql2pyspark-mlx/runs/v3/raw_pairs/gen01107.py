from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("s.customer_id").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.duration_sec"),
        F.col("s.device"),
        F.col("c.segment"),
        F.col("c.signup_date"),
        F.substring(F.col("we.page"), 1, 2).alias("page_prefix"),
    )
    .orderBy("signup_date", "device")
)
