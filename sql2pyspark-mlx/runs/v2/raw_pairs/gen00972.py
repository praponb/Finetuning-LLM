from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("c.city") != "Singapore") | F.col("we.page").isNotNull())
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.event_time"),
        F.col("s.country"),
        F.col("s.browser"),
        F.col("c.credit_limit"),
        F.col("c.is_active"),
        F.when(F.col("s.device") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
)
