from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 50) | (F.col("event_type").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

detail_rows_events = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.hour(F.col("t.event_at")).between(7, 9)), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_slug",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

combined_details_event = (
    detail_rows_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.actor",
        "s.event_type_slug",
        "s.event_id",
        "s.event_event_at",
        "s.event_event_type",
        F.col("t.ticket_id").alias("event_ticket_id"),
        F.col("ti.opened_at").alias("ticket_opened_at"),
        F.col("ti.requester_id").alias("ticket_requester_id"),
    )
)

detail_rows_events_2 = (
    combined_details_event.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type_slug",
        "s.event_id",
        "s.event_event_at",
        "s.event_ticket_id",
        "s.ticket_opened_at",
        "s.ticket_requester_id",
        F.col("t.id").alias("event_id_2"),
        "t.event_at",
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type_2"),
    )
)

rollup_base = (
    detail_rows_events_2
    .groupBy("ticket_id", F.year("ticket_opened_at").alias("ticket_opened_at_year"))
    .agg(
        F.sum(F.when(F.col("event_event_type_2") == "escalated", F.col("ticket_requester_id")).otherwise(0)).alias("ticket_requester_id_event_event_type_2_escalated"),
        F.count("*").alias("row_count"),
        F.round(F.stddev("ticket_requester_id"), 2).alias("stddev_ticket_requester_id"),
        F.max("event_at").alias("last_event_at"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .select(
        "ticket_id",
        "ticket_requester_id_event_event_type_2_escalated",
        "stddev_ticket_requester_id",
        "last_event_at",
        F.dayofweek(F.col("last_event_at")).alias("last_event_at_dayofweek"),
        F.round(F.col("ticket_opened_at_year") / F.nullif(F.col("row_count"), F.lit(0)), 3).alias("ticket_opened_at_year_per_row_count"),
    )
    .orderBy(
        F.col("ticket_opened_at_year_per_row_count").asc_nulls_last(),
        F.col("stddev_ticket_requester_id").asc_nulls_last(),
    )
)
