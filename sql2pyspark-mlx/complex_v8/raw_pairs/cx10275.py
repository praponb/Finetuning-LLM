from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

with_prev = cleaned.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_event_at").isNull() | ((F.unix_timestamp("event_at") - F.unix_timestamp("prev_event_at")) / 60 > 120), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_id", F.sum("new_session_flag").over(w_ticket_id_running))

per_session = (
    sessionized
    .groupBy("ticket_id", "session_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

line_level_events = (
    per_session.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.ticket_id",
        "s.session_id",
        "s.session_start",
        "s.session_end",
        "s.n_events",
        "s.duration_min",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
    )
)

scored = (
    line_level_events
    .select(
        "*",
        F.date_trunc("year", F.col("event_at")).alias("event_at_year"),
        F.round((F.unix_timestamp(F.col("event_at")) - F.unix_timestamp(F.col("session_end"))) / 60, 1).alias("minutes_session_end_to_event_at"),
        F.concat(F.col("event_actor"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_actor_ref"),
    )
)

result = (
    scored
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "ticket_id",
        "session_id",
        "session_start",
        "n_events",
        "duration_min",
        "event_id",
        "event_type",
        "event_at",
        "event_actor",
        "event_actor_ref",
    )
    .orderBy(F.col("ticket_id").desc_nulls_first())
)
