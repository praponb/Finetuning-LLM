from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_burst_id_by_sess = Window.partitionBy("event_count", "burst_id").orderBy("session_start", "event_type", "burst_id")
w_event_count_burst_id_by_sess_2 = Window.partitionBy("event_count", "burst_id").orderBy("session_start", "event_type", "burst_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_event_count_burst_id_by_sess_3 = Window.partitionBy("event_count", "burst_id").orderBy("session_start")

filtered_events = (
    df_ticket_events
    .filter(F.col("event_type") == "resolved")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("starts_session").over(w_event_type_running))

sessions = (
    sessionized
    .groupBy("event_type", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.sum("event_type_len").alias("session_event_type_len"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

with_trends = (
    sessions
    .select(
        "event_type",
        "burst_id",
        "session_start",
        "session_end",
        "session_event_type_len",
        "duration_min",
        F.row_number().over(w_event_count_burst_id_by_sess).alias("seq_no"),
        F.first("burst_id", ignorenulls=True).over(w_event_count_burst_id_by_sess_2).alias("first_known_burst_id"),
        F.round(F.percent_rank().over(w_event_count_burst_id_by_sess), 3).alias("session_start_percent_rank"),
        F.round(F.percent_rank().over(w_event_count_burst_id_by_sess_3), 3).alias("session_start_percent_rank_2"),
        F.round(F.cume_dist().over(w_event_count_burst_id_by_sess_3), 3).alias("session_start_cume_dist"),
    )
)

result = (
    with_trends
    .groupBy("session_start", "first_known_burst_id")
    .pivot("event_type", ["escalated", "created", "assigned", "replied"])
    .agg(F.count("session_start_cume_dist"))
)
