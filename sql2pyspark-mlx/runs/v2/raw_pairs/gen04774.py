from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left_anti")
    .filter(~F.col("s.browser").isin(["Edge", "Firefox", "Safari"]))
    .select(
        F.col("s.id"),
        F.col("s.browser"),
        F.col("s.started_at"),
        F.col("s.customer_id"),
    )
    .orderBy(F.col("id").desc())
)
