from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_semi")
    .filter(F.col("s.device") == "desktop")
    .select(
        F.col("s.id"),
        F.col("s.customer_id"),
        F.col("s.country"),
        F.col("s.browser"),
    )
    .orderBy(F.col("customer_id").desc())
)
