from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered_events = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_trunc("quarter", F.col("event_at")).alias("event_at_quarter"),
    )
)

with_prev = filtered_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 30 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("is_new_session").over(w_ticket_id_running))

session_summary = (
    sessionized
    .groupBy("ticket_id", "session_no")
    .agg(
        F.min("event_at").alias("session_start"),
        F.max("event_at").alias("session_end"),
        F.count("*").alias("event_count"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

without_matches = (
    session_summary.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "refund")), "left_semi")
)

result = (
    without_matches
    .filter(F.col("session_start") >= F.lit("2025-01-21 09:00:00").cast("timestamp"))
    .select("ticket_id", "session_no", "duration_min")
    .orderBy(F.col("session_no").desc(), F.col("duration_min").asc_nulls_last())
)
