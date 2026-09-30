from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("event_at") < F.lit("2025-03-08 09:00:00").cast("timestamp")) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
        F.lower(F.trim(F.col("event_type"))).alias("event_type_clean"),
    )
)

with_details_ticket = (
    valid_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_group",
        "s.event_type_clean",
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.tags").alias("ticket_tags"),
        F.col("t.csat").alias("ticket_csat"),
    )
)

totals = (
    with_details_ticket
    .filter(F.col("ticket_csat") < 4)
    .groupBy("ticket_id")
    .agg(
        F.count(F.when(F.col("event_type") == "created", 1)).alias("n_event_type_created"),
        F.count("*").alias("row_count"),
    )
)

exploded_detail = (
    totals.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-24").cast("date")), "left")
    .select(
        "s.ticket_id",
        "s.n_event_type_created",
        "s.row_count",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

result = (
    exploded_detail
    .filter(F.to_date(F.col("event_at")) < F.lit("2025-03-14").cast("date"))
    .select(
        "ticket_id",
        "n_event_type_created",
        "row_count",
        "event_id",
        "event_actor",
        "event_at",
    )
    .orderBy("row_count", F.col("event_id").desc(), "ticket_id")
    .limit(20)
)
