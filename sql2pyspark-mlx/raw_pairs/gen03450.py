from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type", "page").orderBy("event_time", "id")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

with_windows = (
    df_web_events
    .select(
        "*",
        F.lag("duration_sec").over(w).alias("prev_duration_sec"),
        F.row_number().over(w).alias("seq_no"),
    )
)

result = (
    with_windows
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "duration_sec",
        "prev_duration_sec",
        "seq_no",
        F.col("min_web_event_duration_sec"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "page")
)
