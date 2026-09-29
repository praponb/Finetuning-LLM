from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_ticket_events
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.date_format(F.col("event_at"), "MMM yyyy").alias("event_at_label"),
    )
)

with_prev = prepared.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

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
        F.count("*").alias("n_events"),
        F.round((F.unix_timestamp(F.max("event_at")) - F.unix_timestamp(F.min("event_at"))) / 60, 1).alias("duration_min"),
    )
)

qualified = (
    session_summary.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.requester_id") >= 7), "left_semi")
)

result = (
    qualified
    .rollup("ticket_id", "n_events", "session_no")
    .agg(F.count("*").alias("cnt"))
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("n_events").asc_nulls_last(),
        F.col("session_no").asc_nulls_last(),
    )
)
