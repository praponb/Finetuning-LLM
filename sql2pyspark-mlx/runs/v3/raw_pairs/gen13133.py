from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "inner")
    .join(df_customers.alias("c"), F.col("s.customer_id") == F.col("c.id"), "inner")
    .filter((F.col("we.duration_sec") == 339) & F.col("we.page").isin(["/search", "/home"]))
    .select(
        F.col("we.duration_sec"),
        F.col("s.browser"),
        F.col("c.city"),
        F.to_date(F.col("we.event_time")).alias("event_time_date"),
    )
    .orderBy(F.col("browser").desc())
)
