from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("page").orderBy("duration_sec", "id").rowsBetween(-6, -1)
w_ordered = Window.partitionBy(F.month("event_time"), "session_id").orderBy("duration_sec_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id", "page").orderBy(F.col("first_known_duration_sec").desc(), "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id")
w5 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("page").orderBy("event_time", "id")
w7 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_web_events
    .filter(F.col("event_time") > F.lit("2023-08-08").cast("date"))
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        F.coalesce("duration_sec", F.lit(0)).alias("duration_sec_filled"),
        F.coalesce("page", F.lit("DIRECT")).alias("resolved_page"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("duration_sec_filled") >= 74)
    .select(
        "id",
        "session_id",
        "page",
        "event_time",
        "duration_sec",
        "duration_sec_filled",
        "resolved_page",
        F.avg("duration_sec").over(w_hist).alias("duration_sec_avg_6_prior"),
        F.first("duration_sec", ignorenulls=True).over(w_ordered).alias("first_known_duration_sec"),
    )
)

latest = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "session_id", "page", "event_time", "duration_sec_filled")
)

with_gaps = (
    latest
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w4) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("session_start_flag").over(w5))

flagged = (
    sessions_web_events
    .withColumn("session_start_flag_2", F.when(F.lag("event_time").over(w6).isNull() | (F.col("event_time") > F.lag("event_time").over(w6) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events_2 = flagged.withColumn("burst_id", F.sum("session_start_flag_2").over(w7))

joined = (
    sessions_web_events_2
    .alias("s")
    .join(df_sessions.alias("d"), F.col("s.session_id") == F.col("d.id"), "inner")
    .select(
        "s.id",
        "s.session_id",
        "s.event_time",
        "s.duration_sec_filled",
        "s.session_id_2",
        F.col("d.device"),
        F.col("d.country"),
    )
)

result = (
    joined
    .cube(
        F.date_trunc("MONTH", "event_time").alias("activity_month"),
        "session_id",
        F.date_trunc("QUARTER", "event_time").alias("quarter_start"),
    )
    .agg(
        F.max("event_time").alias("last_event_time"),
        F.countDistinct("country").alias("distinct_countries"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_rows"),
    )
)
