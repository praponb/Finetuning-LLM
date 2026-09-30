from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-13 13:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

enriched = (
    filtered_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_clean",
        F.col("t.channel").alias("ticket_channel"),
    )
)

result = (
    enriched
    .filter(F.col("ticket_id").isNotNull())
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_clean",
        "ticket_channel",
        F.initcap(F.trim(F.col("actor_clean"))).alias("actor_clean_clean"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
    .orderBy("actor_clean_clean", "ticket_id")
)
