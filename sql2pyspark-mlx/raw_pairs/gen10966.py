from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(F.count(F.when(F.col("we.page") == "/search", 1)).alias("search_count"))
    .filter(F.col("search_count") > 5)
)
