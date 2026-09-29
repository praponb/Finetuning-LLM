from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_web_events
    .filter(F.col("event_time") >= "2023-11-01")
    .withColumn("days_since_event_time", F.datediff(F.current_date(), "event_time"))
    .withColumn("page_filled", F.coalesce("page", F.lit("OTHER")))
)

gaps = (
    valid
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

with_gaps = (
    sessions_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events_2 = with_gaps.withColumn("burst_id", F.sum("new_group_flag").over(w4))

remaining_keys = (
    sessions_web_events_2
    .select("page", "id")
    .subtract(
        df_web_events
        .filter(F.col("duration_sec").between(235, 281))
        .select("page", "id")
    )
)

verified = (
    sessions_web_events_2
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.page") == F.col("k.page")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "event_type",
        "page",
        "duration_sec",
        "page_filled",
        "session_start_flag",
        "new_group_flag",
        "burst_id",
    )
    .orderBy("id", "event_type")
)
