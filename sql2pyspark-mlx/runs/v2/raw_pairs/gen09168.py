from pyspark.sql import functions as F

result = (
    df_web_events
    .filter(~F.col("page").isin(["/checkout", "/search"]))
    .select(
        "id",
        "duration_sec",
        "session_id",
        F.length("page").alias("page_length"),
        F.hour("event_time").alias("event_time_hour"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
