from pyspark.sql import functions as F

cleaned_events = (
    df_ticket_events
    .filter((F.col("ticket_id") >= 7) & (F.col("event_type") == "resolved"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
    )
)

line_level = (
    cleaned_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("customer", "agent")), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor_clean",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_at").alias("event_event_at"),
    )
)

scored = (
    line_level
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "event_type",
        "actor_clean",
        "event_id",
        "event_actor",
        "event_event_at",
        F.length(F.trim(F.col("actor_clean"))).alias("actor_clean_len"),
    )
)

exploded_detail = (
    scored.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_id",
        "s.event_actor",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor_2"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "ticket_id",
        "event_type",
        "event_id",
        "event_actor_2",
        "event_event_type",
        F.length(F.trim(F.col("event_type"))).alias("event_type_len"),
        F.coalesce(F.col("event_actor_2"), F.lit("UNKNOWN")).alias("event_actor_2_or_unknown"),
    )
    .orderBy(F.col("event_actor_2_or_unknown").asc_nulls_last())
)
