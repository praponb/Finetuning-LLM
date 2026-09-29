from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id")
w3 = Window.partitionBy("session_id").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w4 = Window.partitionBy(F.to_date("event_time"), "page").orderBy("event_time", "id")
w5 = Window.partitionBy("page", "event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.min("duration_sec").alias("min_web_event_duration_sec"))

enriched = (
    df_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "duration_sec",
        F.sum("duration_sec").over(w).alias("page_sum_duration_sec"),
        F.ntile(4).over(w2).alias("duration_sec_bucket"),
        F.count("duration_sec").over(w3).alias("duration_sec_count_7d"),
        F.row_number().over(w4).alias("event_seq"),
        F.sum("duration_sec").over(w5).alias("cumulative_duration_sec"),
    )
)

result = (
    enriched
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "page_sum_duration_sec",
        "duration_sec_bucket",
        "duration_sec_count_7d",
        "event_seq",
        F.col("min_web_event_duration_sec"),
    )
)
