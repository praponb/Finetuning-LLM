from pyspark.sql import functions as F

result = (
    df_web_events.alias("we")
    .join(df_sessions.alias("s"), F.col("s.id") == F.col("we.session_id"), "right")
    .groupBy(F.col("s.browser"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set(F.col("we.page")))).alias("page_values"),
    )
    .orderBy(F.col("browser").desc())
)
