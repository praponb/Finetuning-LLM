from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("browser").orderBy("started_at", "id")
w2 = Window.partitionBy("browser").orderBy("started_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.event_time").desc(), F.col("c.id").desc())
w4 = Window.partitionBy("country").orderBy("burst_id", "id")
w_recent = Window.partitionBy("browser").orderBy("prev_web_event_duration_sec", "id")
w_recent_2 = Window.partitionBy("country").orderBy("next_burst_id", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_sessions
    .filter(F.col("started_at") > F.lit("2023-07-15").cast("date"))
    .select(
        "id",
        "device",
        "browser",
        "started_at",
        "country",
        F.trim("browser").alias("clean_browser"),
    )
)

with_gaps = (
    filtered
    .withColumn("is_new_burst", F.when(F.lag("started_at").over(w).isNull() | (F.col("started_at") > F.lag("started_at").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessions_sessions = (
    with_gaps
    .select(
        "id",
        "browser",
        "started_at",
        "country",
        F.sum("is_new_burst").over(w2).alias("burst_id"),
    )
)

with_last = (
    sessions_sessions
    .alias("s")
    .join(df_web_events.alias("c"), (F.col("c.session_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.duration_sec").alias("web_event_duration_sec"))
)

with_windows = (
    with_last
    .select(
        "*",
        F.lead("burst_id").over(w4).alias("next_burst_id"),
        F.lag("burst_id").over(w4).alias("prev_burst_id"),
        F.lag("web_event_duration_sec").over(w4).alias("prev_web_event_duration_sec"),
        F.lead("web_event_duration_sec").over(w4).alias("next_web_event_duration_sec"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    with_windows
    .filter(F.to_date("started_at") < "2024-03-01")
    .select(
        "id",
        "country",
        "web_event_duration_sec",
        "prev_web_event_duration_sec",
        "next_web_event_duration_sec",
        "row_seq",
        F.lag("prev_burst_id").over(w_recent).alias("prev_prev_burst_id"),
        F.lead("next_burst_id").over(w_recent).alias("next_next_burst_id"),
        F.row_number().over(w_recent).alias("row_seq_2"),
        F.first("next_web_event_duration_sec", ignorenulls=True).over(w_recent_2).alias("first_known_next_web_event_duration_sec"),
    )
)
