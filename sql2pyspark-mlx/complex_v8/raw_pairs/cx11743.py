from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_actor = Window.partitionBy("actor").orderBy("event_at", "id")
w_actor_running = Window.partitionBy("actor").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

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

with_prev = scoped_events.withColumn("prev_event_at", F.lag("event_at").over(w_actor))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 30), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_actor_running))

per_session = (
    sessionized
    .groupBy("actor", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

with_activity = (
    per_session
    .crossJoin(per_session.agg(F.max("session_no").alias("overall_max_session_no")))
    .filter(F.col("session_no") < F.col("overall_max_session_no"))
    .drop("overall_max_session_no")
)

totals = (
    with_activity
    .groupBy(
        "session_no",
        "n_events",
        F.to_date("session_start").alias("session_start_day"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("actor") != "system", F.col("n_events")).otherwise(0)).alias("n_events_actor_system"),
        F.round(100 * F.count(F.when(F.col("actor") == "system", 1)) / F.count("*"), 1).alias("pct_actor_system"),
    )
)

result = (
    totals
    .select(
        "session_no",
        "n_events",
        "session_start_day",
        "pct_actor_system",
    )
)
