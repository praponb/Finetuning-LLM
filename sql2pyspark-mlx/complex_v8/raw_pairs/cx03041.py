from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ticket_id_events_by_events = Window.partitionBy("ticket_id", "events").orderBy(F.col("events").desc(), "ticket_id", "session_no")
w_ticket_id_events_by_events_r = Window.partitionBy("ticket_id", "events").orderBy(F.col("events").desc(), "ticket_id", "session_no").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events = Window.partitionBy("events")
w_ticket_id_events_by_events_m = Window.partitionBy("ticket_id", "events").orderBy(F.col("events").desc(), "ticket_id", "session_no").rowsBetween(-1, Window.currentRow)

cleaned_events = (
    df_ticket_events
    .filter(F.col("ticket_id") >= 39)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

with_prev = cleaned_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 90 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_ticket_id_running))

sessions = (
    sessionized
    .groupBy("ticket_id", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

windowed = (
    sessions
    .select(
        "ticket_id",
        "session_no",
        "session_start",
        "duration_min",
        F.rank().over(w_ticket_id_events_by_events).alias("events_rank"),
        F.min("duration_min").over(w_ticket_id_events_by_events_r).alias("min_duration_min_so_far"),
        F.count(F.lit(1)).over(w_events).alias("rows_in_events"),
        F.sum("events").over(w_ticket_id_events_by_events_m).alias("events_sum_2"),
    )
)

result = (
    windowed
    .select(
        "ticket_id",
        "session_no",
        "session_start",
        "duration_min",
        "events_rank",
        "min_duration_min_so_far",
        "rows_in_events",
        "events_sum_2",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("rows_in_events").asc_nulls_last(),
    )
)
