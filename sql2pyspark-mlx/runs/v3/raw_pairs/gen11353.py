from pyspark.sql import functions as F

session_summary = (
    df_web_events
    .filter(~F.col("page").isin(["/search", "/product", "/checkout"]))
    .groupBy("session_id")
    .agg(
        F.countDistinct("duration_sec").alias("unique_pages"),
        F.max("duration_sec").alias("max_duration_sec"),
    )
)

result = (
    df_sessions.alias("s")
    .join(session_summary.alias("ss"), F.col("s.id") == F.col("ss.session_id"), "left")
    .select(F.col("s.browser"), F.col("ss.unique_pages"), F.col("ss.max_duration_sec"))
    .orderBy(F.col("max_duration_sec").desc())
)
