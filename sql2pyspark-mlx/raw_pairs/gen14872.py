from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter(F.col("we.duration_sec").between(190, 436))
    .select(F.col("s.country"), F.col("we.page"))
)
