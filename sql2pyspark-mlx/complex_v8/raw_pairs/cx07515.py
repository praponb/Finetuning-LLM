from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = df_ticket_events

with_prev = base.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 2 HOURS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_ticket_id_running))

per_session = (
    sessionized
    .groupBy("ticket_id", "burst_id")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

filtered = (
    per_session.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.priority") == "P1"), "left_semi")
)

without_matches = (
    filtered.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor") == "agent") & (F.to_date(F.col("t.event_at")) < F.lit("2025-03-14").cast("date")), "left_anti")
    .filter(F.col("ticket_id").isNotNull())
)

result = (
    without_matches
    .select(
        "ticket_id",
        "burst_id",
        "session_start",
        "session_end",
        "n_events",
        "duration_min",
        F.round(F.col("duration_min") * 0.25, 2).alias("duration_min_adj"),
        F.round((F.unix_timestamp(F.col("session_end")) - F.unix_timestamp(F.col("session_start"))) / 60, 1).alias("minutes_session_start_to_session_end"),
    )
    .orderBy(F.col("minutes_session_start_to_session_end").desc())
)
