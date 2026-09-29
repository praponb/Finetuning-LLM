from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("actor", "ticket_agent_id").orderBy(F.col("event_at").desc(), F.col("id").desc())

scoped_events = (
    df_ticket_events
    .filter(F.col("event_type") == "assigned")
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd HH:00").alias("event_at_label"),
    )
)

combined_details = (
    scoped_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.event_type_or_unknown",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

latest = (
    combined_details
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded_detail = (
    latest.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_at") >= F.lit("2025-01-25 13:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.ticket_closed_at",
        "s.ticket_agent_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
    )
)

with_details_ticket = (
    exploded_detail.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_ticket_events.alias("ti"), F.col("ti.id") == F.col("s.event_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.ticket_closed_at",
        "s.ticket_agent_id",
        "s.event_id",
        "s.event_event_at",
        "s.event_actor",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.priority").alias("ticket_priority"),
        "ti.event_at",
        F.col("ti.actor").alias("event_actor_2"),
    )
)

result = (
    with_details_ticket
    .filter(F.to_date(F.col("ticket_opened_at")) < F.lit("2025-02-12").cast("date"))
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor",
        "ticket_closed_at",
        "ticket_agent_id",
        "event_id",
        "event_event_at",
        "event_actor",
        "ticket_opened_at",
        "ticket_priority",
        "event_actor_2",
        F.coalesce(F.col("event_type"), F.lit("UNKNOWN")).alias("event_type_or_unknown"),
    )
    .orderBy(
        F.col("event_actor_2").desc(),
        F.col("ticket_priority").desc_nulls_first(),
        "id",
        "event_id",
    )
    .limit(20)
)
