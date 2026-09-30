from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("event_type").orderBy(F.col("duration_sec").desc(), "id")
w2 = Window.partitionBy("event_type").orderBy("duration_sec", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("session_id").orderBy("event_seq", "id")

scored = (
    df_web_events
    .select(
        "id",
        "session_id",
        "page",
        F.row_number().over(w).alias("event_seq"),
        F.last("duration_sec", ignorenulls=True).over(w2).alias("last_known_duration_sec"),
        F.avg("duration_sec").over(w3).alias("duration_sec_avg_30d"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_sessions.alias("b"), F.col("src.session_id") == F.col("b.id"), "left")
    .filter(F.col("b.country") == "JP")
    .select("src.*", F.col("b.country"), F.col("b.device"))
)

joined = (
    with_dims
    .alias("src")
    .join(df_sessions.alias("s"), F.col("src.session_id") == F.col("s.id"), "left")
    .select(
        "src.id",
        "src.session_id",
        "src.page",
        "src.event_seq",
        "src.last_known_duration_sec",
        "src.country",
        "src.device",
        F.col("s.device").alias("session_device"),
    )
)

with_windows = (
    joined
    .select(
        "*",
        F.lag("last_known_duration_sec").over(w4).alias("prev_last_known_duration_sec"),
        F.lead("last_known_duration_sec").over(w4).alias("next_last_known_duration_sec"),
        F.lead("event_seq").over(w4).alias("next_event_seq"),
        F.lag("event_seq").over(w4).alias("prev_event_seq"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

result = (
    with_windows
    .alias("s")
    .join(df_sessions.alias("p"), F.col("s.session_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["session_country", "session_device", "web_event_device"], ["session_country", "session_device"], ["session_country"], []],
        F.col("p.country").alias("session_country"),
        F.col("p.device").alias("session_device"),
        F.col("s.device").alias("web_event_device"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("s.prev_last_known_duration_sec")), 2).alias("avg_prev_last_known_duration_sec"),
    )
)
