from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("event_type").orderBy(F.col("prev_event_time").desc_nulls_last(), "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id")
w5 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("event_type").orderBy(F.col("event_time").desc(), "id")
w7 = Window.partitionBy("session_id").orderBy("session_id_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("page").orderBy("rank_in_group", "id")
w9 = Window.partitionBy("page").orderBy(F.col("prev_session_id_3").asc_nulls_last(), "id")
w10 = Window.partitionBy("page").orderBy("is_new_burst", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("row_num", "id")

prev_events = df_web_events.withColumn("prev_event_time", F.lag("event_time").over(w))

with_gaps = (
    prev_events
    .withColumn("new_group_flag", F.when(F.col("prev_event_time").isNull() | (F.col("event_time") > F.col("prev_event_time") + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events = with_gaps.withColumn("session_id_2", F.sum("new_group_flag").over(w2))

ranked = (
    sessions_web_events
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

flagged = (
    ranked
    .withColumn("is_new_burst", F.when(F.lag("event_time").over(w4).isNull() | (F.col("event_time") > F.lag("event_time").over(w4) + F.expr("INTERVAL 60 DAYS")), 1).otherwise(0))
)

sessions_web_events_2 = flagged.withColumn("session_id_3", F.sum("is_new_burst").over(w5))

latest = (
    sessions_web_events_2
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "session_id",
        "page",
        "session_id_2",
        "rank_in_group",
        "is_new_burst",
        "session_id_3",
    )
)

with_windows = (
    latest
    .filter(F.col("page") != "/product")
    .select(
        "id",
        "page",
        "rank_in_group",
        "is_new_burst",
        "session_id_3",
        F.sum("session_id_2").over(w7).alias("cumulative_session_id_2"),
        F.lag("session_id_3", 1).over(w8).alias("prev_session_id_3"),
    )
)

latest_web_events = (
    with_windows
    .withColumn("row_num", F.row_number().over(w9))
    .filter(F.col("row_num") == 1)
)

scored = (
    latest_web_events
    .select(
        "id",
        "page",
        "is_new_burst",
        "cumulative_session_id_2",
        "prev_session_id_3",
        "row_num",
        F.sum("is_new_burst").over(w10).alias("cumulative_is_new_burst"),
        F.lag("cumulative_session_id_2", 1, 0).over(w11).alias("prev_cumulative_session_id_2"),
    )
)

result = (
    scored
    .select(
        "id",
        "page",
        "cumulative_session_id_2",
        "prev_session_id_3",
        "prev_cumulative_session_id_2",
    )
)
