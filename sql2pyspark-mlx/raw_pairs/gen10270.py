from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("session_id").orderBy("event_time", "id")
w2 = Window.partitionBy("event_type", "page").orderBy("duration_sec", "id")
w3 = Window.partitionBy("event_type").orderBy("event_time", "id")
w4 = Window.partitionBy("event_type").orderBy("event_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("event_type", "session_id").orderBy("event_time", "id").rowsBetween(-6, Window.currentRow)
duration_sec_by_session_id = df_web_events.groupBy("session_id").agg(F.avg("duration_sec").alias("avg_web_event_duration_sec"))

deduped = (
    df_web_events
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

ranked = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "session_id",
        "event_type",
        "page",
        "event_time",
        "duration_sec",
        "rn",
    )
)

with_gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("event_time") - F.lag("event_time").over(w3) > F.expr("INTERVAL 90 DAYS"), 1).otherwise(0))
)

sessions_web_events = (
    with_gaps
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "rn",
        F.sum("is_new_burst").over(w4).alias("session_no"),
    )
)

scored = (
    sessions_web_events
    .filter(F.col("session_no").between(2, 6))
    .withColumn("last7_max_session_no", F.max("session_no").over(w_recent))
    .withColumn("last7_sum_session_no", F.sum("session_no").over(w_recent))
    .withColumn("last7_min_session_no", F.min("session_no").over(w_recent))
)

result = (
    scored
    .join(duration_sec_by_session_id, on="session_id", how="left")
    .select(
        "id",
        "session_id",
        "event_type",
        "event_time",
        "rn",
        "session_no",
        F.when(F.col("event_type").rlike("^(purchase|add_to_cart)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("event_type_zone"),
        F.col("avg_web_event_duration_sec"),
    )
    .orderBy("rn", "event_time")
)
