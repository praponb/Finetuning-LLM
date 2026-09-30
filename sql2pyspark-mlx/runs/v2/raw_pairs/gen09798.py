from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "left")
    .filter((F.col("s.device") != "tablet") & F.col("s.device").isin(["desktop", "tablet"]))
    .select(F.col("we.page"), F.col("s.browser"), F.col("s.device"))
    .orderBy(F.col("device").desc(), F.col("page").desc())
)
