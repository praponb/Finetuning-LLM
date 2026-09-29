from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_session_end = Window.orderBy(F.col("session_end").asc_nulls_last(), "ticket_id", "session_no")
w_ticket_id_2 = Window.partitionBy("ticket_id")
w_session_no_session_end_seq_b = Window.partitionBy("session_no", "session_end_seq").orderBy("session_start", "ticket_id", "session_no")

recent_events = (
    df_ticket_events
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-02-06").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("actor"), F.lit("UNKNOWN")).alias("actor_or_unknown"),
    )
)

with_prev = recent_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    sessions
    .withColumn("session_end_rank", F.rank().over(w_all_by_session_end))
    .withColumn("session_end_seq", F.row_number().over(w_all_by_session_end))
)

metrics = (
    ranked
    .withColumn("ticket_id_avg_session_end_rank", F.round(F.avg("session_end_rank").over(w_ticket_id_2), 2))
    .withColumn("seq_no", F.row_number().over(w_session_no_session_end_seq_b))
)

result = (
    metrics
    .filter(F.col("session_end_seq") > 3)
    .select(
        "ticket_id",
        "session_no",
        "session_start",
        "session_end",
        "event_count",
        "session_end_rank",
        "session_end_seq",
        "ticket_id_avg_session_end_rank",
        "seq_no",
        F.round((F.unix_timestamp(F.col("session_start")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_session_start"),
        F.date_format(F.col("session_end"), "MMM yyyy").alias("session_end_label"),
    )
)
