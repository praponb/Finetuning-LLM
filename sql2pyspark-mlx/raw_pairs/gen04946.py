from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy(F.col("event_time").desc(), "id")

with_gaps = (
    df_web_events
    .withColumn("new_group_flag", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

islands = (
    with_gaps
    .select(
        "id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        F.sum("new_group_flag").over(w2).alias("session_id_2"),
    )
)

latest = (
    islands
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

with_prev = latest.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("burst_id", F.sum("is_new_session").over(w2))

flagged_2 = (
    sessions_web_events
    .withColumn("session_start_flag", F.when(F.col("event_time") - F.lag("event_time").over(w) > F.expr("INTERVAL 14 DAYS"), 1).otherwise(0))
)

sessions_web_events_2 = (
    flagged_2
    .select(
        "id",
        "page",
        "event_time",
        "duration_sec",
        "session_id_2",
        "row_num",
        "prev_event_time",
        "burst_id",
        F.sum("session_start_flag").over(w2).alias("session_no"),
    )
)

result = (
    sessions_web_events_2
    .select("id", "page", "duration_sec", "session_no")
    .orderBy(F.col("id").desc_nulls_first())
)
