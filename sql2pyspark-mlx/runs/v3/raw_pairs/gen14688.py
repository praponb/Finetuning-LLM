from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("we.session_id") == F.col("s.id"), "left")
    .select(
        F.col("we.id"),
        F.col("we.page"),
        F.col("we.session_id"),
        F.col("we.duration_sec"),
        F.col("s.country"),
        F.initcap(F.trim(F.col("s.country"))).alias("country_title"),
    )
    .orderBy("page", F.col("id").desc())
    .limit(5)
)
