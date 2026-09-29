from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("we.session_id") == F.col("s.id"), "left")
    .filter(F.col("s.browser").isin(["Edge", "Firefox"]))
    .select(F.col("s.country"), F.col("we.session_id"), F.col("we.page"))
    .orderBy("page")
)
