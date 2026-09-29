from pyspark.sql import functions as F

result = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        F.col("duration_sec").cast("string").alias("duration_sec_str"),
        F.date_format("event_time", "yyyy-MM-dd").alias("event_time_label"),
    )
    .orderBy(F.col("id").desc(), "page")
    .limit(20)
)
