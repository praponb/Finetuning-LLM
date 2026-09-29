from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_events_actor_by_events_ties = Window.partitionBy("events", "actor").orderBy(F.col("events").desc())
w_session_no_by_events = Window.partitionBy("session_no").orderBy("events", "actor", "session_no")

filtered_events = (
    df_ticket_events
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_actor_running))

session_summary = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

sequenced = (
    session_summary
    .withColumn("events_percent_rank", F.round(F.percent_rank().over(w_events_actor_by_events_ties), 3))
    .withColumn("minutes_since_prev_session_end", F.round((F.unix_timestamp("session_end") - F.unix_timestamp(F.lag("session_end").over(w_session_no_by_events))) / 60, 1))
)

scored = (
    sequenced
    .select(
        "*",
        F.quarter(F.col("session_end")).alias("session_end_quarter"),
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 3600, 1).alias("hours_session_start_to_session_end"),
    )
)

result = (
    scored
    .select(
        "actor",
        "session_no",
        "session_end",
        "events",
        "duration_min",
        "events_percent_rank",
        "minutes_since_prev_session_end",
        "session_end_quarter",
        "hours_session_start_to_session_end",
        F.substring(F.trim(F.col("actor")), 1, 2).alias("actor_prefix"),
    )
    .orderBy(F.col("session_end_quarter").asc())
)
