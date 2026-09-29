from pyspark.sql import functions as F

result = (
    df_sessions.alias("s")
    .join(df_web_events.alias("we"), F.col("s.id") == F.col("we.session_id"), "inner")
    .filter(F.col("we.page").isin(["/home", "/cart", "/search"]))
    .groupBy(F.col("s.browser"))
    .agg(F.count(F.col("we.id")).alias("web_event_count"))
)
