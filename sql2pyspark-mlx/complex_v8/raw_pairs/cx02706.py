from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_id = Window.partitionBy("ticket_id").orderBy("event_at", "id")
w_ticket_id_running = Window.partitionBy("ticket_id").orderBy("event_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_events = (
    df_ticket_events
    .filter(F.col("ticket_id") <= 13)
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
    )
)

with_prev = prepared_events.withColumn("prev_event_at", F.lag("event_at").over(w_ticket_id))

flagged = (
    with_prev
    .withColumn("is_new_session", F.when(F.col("prev_event_at").isNull() | (F.col("event_at") > F.col("prev_event_at") + F.expr("INTERVAL 45 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("is_new_session").over(w_ticket_id_running))

enriched_ticket = (
    sessionized.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.event_type_len",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.tags").alias("ticket_tags"),
    )
)

result = (
    enriched_ticket
    .cube("ticket_id", "event_type", "event_type_len")
    .agg(
        F.count("*").alias("row_count"),
        F.sum("event_type_len").alias("total_event_type_len"),
    )
    .select(
        "ticket_id",
        F.coalesce(F.col("event_type"), F.lit("ALL")).alias("event_type"),
        "event_type_len",
        "row_count",
        "total_event_type_len",
    )
    .orderBy(
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
        F.col("event_type_len").asc_nulls_last(),
    )
)
