from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("we.duration_sec") < 290) & F.col("we.duration_sec").between(246, 334))
    .select(
        F.col("s.country"),
        F.col("s.customer_id"),
        F.col("we.event_type"),
        F.initcap(F.trim(F.col("s.browser"))).alias("browser_title"),
    )
    .orderBy("browser_title")
)
