from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("duration_sec", "id")
w4 = Window.partitionBy("country", "page").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w5 = Window.partitionBy(F.date_trunc("MONTH", "event_time"), "country").orderBy("event_time", "id")
w6 = Window.partitionBy(F.date_trunc("month", "event_time"), "page").orderBy(F.col("duration_sec").desc(), "id")
w7 = Window.partitionBy(F.to_date("event_time"), "country").orderBy(F.col("event_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w8 = Window.partitionBy("country").orderBy("event_time", "id")
w9 = Window.partitionBy("country").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("duration_sec").between(231, 235))
    .select(
        "*",
        F.when(F.col("event_type") == "click", 1).otherwise(0).alias("is_click"),
        F.coalesce("page", F.lit("DIRECT")).alias("page_filled"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 60 DAYS"), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.sum("session_start_flag").over(w2).alias("burst_id"),
    )
)

joined = (
    islands
    .alias("e")
    .join(df_sessions.alias("d"), F.col("e.session_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "page", "event_time", "duration_sec", "country")
)

with_windows = (
    latest
    .select(
        "*",
        F.sum("duration_sec").over(w4).alias("duration_sec_sum_30d"),
        F.lag("duration_sec", 2).over(w5).alias("prev_duration_sec"),
        F.ntile(10).over(w6).alias("duration_sec_bucket"),
        F.max("duration_sec").over(w7).alias("duration_sec_max_7d"),
    )
)

flagged = (
    with_windows
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w8).isNull() | (F.col("event_time") > F.lag("event_time").over(w8) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands_web_events = (
    flagged
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "country",
        "duration_sec_sum_30d",
        "prev_duration_sec",
        "duration_sec_max_7d",
        F.sum("is_new_session").over(w9).alias("island_id"),
    )
)

result = (
    islands_web_events
    .select(
        "id",
        "page",
        "duration_sec",
        "country",
        "duration_sec_sum_30d",
        "prev_duration_sec",
        "duration_sec_max_7d",
        "island_id",
    )
    .orderBy(F.col("prev_duration_sec").desc())
)
