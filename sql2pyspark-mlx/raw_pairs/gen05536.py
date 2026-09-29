from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("session_id").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("page").orderBy("event_time", "id")
w4 = Window.partitionBy("page").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("session_id", "event_type").orderBy("burst_id_2", "id")
w6 = Window.partitionBy("page").orderBy("weeks_since_event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_gaps = (
    df_web_events
    .withColumn("is_new_session", F.when(F.lag("event_time").over(w).isNull() | (F.col("event_time") > F.lag("event_time").over(w) + F.expr("INTERVAL 14 DAYS")), 1).otherwise(0))
)

islands = with_gaps.withColumn("burst_id", F.sum("is_new_session").over(w2))

flagged = (
    islands
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 30 DAYS"), 1).otherwise(0))
)

islands_web_events = flagged.withColumn("burst_id_2", F.sum("is_new_burst").over(w4))

filtered = (
    islands_web_events
    .filter(F.col("page").isNotNull() & (F.col("event_time") >= "2023-07-01"))
    .select(
        "*",
        (F.col("is_new_burst") * F.col("burst_id_2")).alias("is_new_burst_times_burst_id_2"),
        (F.datediff(F.current_date(), "event_time") / 7).alias("weeks_since_event_time"),
    )
)

enriched = (
    filtered
    .filter(F.col("is_new_burst") != 1)
    .withColumn("prev_weeks_since_event_time", F.lag("weeks_since_event_time").over(w_hist))
    .withColumn("next_weeks_since_event_time", F.lead("weeks_since_event_time").over(w_hist))
    .withColumn("prev_is_new_session", F.lag("is_new_session").over(w_hist))
    .withColumn("next_is_new_session", F.lead("is_new_session").over(w_hist))
    .withColumn("event_seq", F.row_number().over(w_hist))
    .withColumn("running_duration_sec", F.sum("duration_sec").over(w6))
)

with_gaps_2 = (
    enriched
    .withColumn("new_group_flag", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

sessionized = with_gaps_2.withColumn("session_id_2", F.sum("new_group_flag").over(w4))

result = (
    sessionized
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "duration_sec",
        "is_new_session",
        "burst_id",
        "is_new_burst",
        "burst_id_2",
        F.when(F.col("event_type").rlike("^(add_to_cart|page_view)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("event_type_zone"),
    )
)
