from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")
w2 = Window.partitionBy("page", "event_type").orderBy("event_time", "id")
w3 = Window.orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("event_type")
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

cleaned = (
    df_web_events
    .filter((F.col("event_time") >= F.lit("2023-06-07 20:00:00").cast("timestamp")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        F.concat("event_type", F.lit(", "), "page").alias("event_type_page"),
        F.coalesce("page", F.lit("UNKNOWN")).alias("page_filled"),
    )
)

filtered = (
    cleaned
    .filter((F.col("event_time") >= F.lit("2023-09-30").cast("date")) & F.col("page").isNotNull())
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "event_type_page",
        "page_filled",
        F.lower("event_type").alias("event_type_lower"),
        F.coalesce("page", F.lit("N/A")).alias("page_clean"),
    )
)

with_windows = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

metrics = (
    with_windows
    .select(
        "*",
        F.sum("seq_no").over(w3).alias("running_seq_no"),
        F.sum("seq_no").over(w4).alias("running_seq_no_2"),
        F.avg("row_seq").over(w5).alias("row_seq_avg_6"),
        F.min("row_seq").over(w6).alias("event_type_min_row_seq"),
    )
)

result = (
    metrics
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "event_type_page",
        "page_clean",
        "seq_no",
        "row_seq_avg_6",
        "event_type_min_row_seq",
        F.col("avg_web_event_duration_sec"),
    )
)
