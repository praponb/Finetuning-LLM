from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.filter(F.col("country").isin(["UK", "JP", "TH"])).alias("s"), F.col("we.session_id") == F.col("s.id"), "left_semi")
    .filter(F.col("we.page").isin(["/search", "/cart"]))
    .select(F.col("we.id"), F.col("we.duration_sec"), F.col("we.event_type"))
    .orderBy(F.col("event_type").desc())
)
