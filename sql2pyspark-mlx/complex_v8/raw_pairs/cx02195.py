from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_type = Window.partitionBy("event_type").orderBy("event_at", "id")
w_event_type_running = Window.partitionBy("event_type").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_n_events_full = Window.orderBy("n_events", "event_type", "session_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_n_events = Window.partitionBy("n_events")

scoped_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_event_type))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 90), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("starts_session").over(w_event_type_running))

per_session = (
    sessionized
    .groupBy("event_type", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

ranked = (
    per_session
    .select(
        "event_type",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.first("n_events").over(w_all_by_n_events_full).alias("first_n_events"),
        F.size(F.collect_set("event_type").over(w_n_events)).alias("distinct_event_type_in_n_events"),
    )
)

result = (
    ranked
    .select(
        "event_type",
        "session_id",
        "session_start",
        "session_end",
        "n_events",
        F.to_date(F.col("session_end")).alias("session_end_date"),
    )
)
