from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("page").orderBy("event_time", "id")
w2 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("session_id").orderBy("event_time", "id")
w4 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("event_type").orderBy(F.col("burst_id").desc())
w6 = Window.orderBy("burst_id", "id")
w7 = Window.partitionBy("page").orderBy("new_group_flag", "id")
w8 = Window.partitionBy("event_type").orderBy("new_group_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("page").orderBy("prev_event_time", "id")
w10 = Window.partitionBy("page").orderBy("prev_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.partitionBy("event_type").orderBy("event_time", "id")
w12 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

flagged = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

sessions_web_events = flagged.withColumn("session_no", F.sum("new_group_flag").over(w2))

gaps = (
    sessions_web_events
    .withColumn("new_group_flag_2", F.when(F.lag("event_time").over(w3).isNull() | (F.col("event_time") > F.lag("event_time").over(w3) + F.expr("INTERVAL 7 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("burst_id", F.sum("new_group_flag_2").over(w4))

enriched = (
    sessionized
    .select(
        "*",
        F.rank().over(w5).alias("burst_id_rank"),
        F.lead("session_no", 1).over(w6).alias("next_session_no"),
        F.lag("session_no", 2, 0).over(w7).alias("prev_session_no"),
        F.last("duration_sec", ignorenulls=True).over(w8).alias("last_known_duration_sec"),
        F.dense_rank().over(w5).alias("burst_id_dense_rank"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_sessions.alias("s"), F.col("a.session_id") == F.col("s.id"), "inner")
    .select("a.*", F.col("s.browser"))
)

flagged_2 = (
    joined
    .withColumn("new_group_flag_3", F.when(F.col("prev_event_time") - F.lag("prev_event_time").over(w9) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

sessions_web_events_2 = flagged_2.withColumn("session_id_2", F.sum("new_group_flag_3").over(w10))

with_gaps = (
    sessions_web_events_2
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w11).isNull() | (F.col("event_time") > F.lag("event_time").over(w11) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_web_events = with_gaps.withColumn("island_id", F.sum("is_new_session").over(w12))

result = (
    sessionized_web_events
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "new_group_flag",
        "session_no",
        "burst_id",
    )
    .orderBy(F.col("new_group_flag").desc_nulls_first())
)
