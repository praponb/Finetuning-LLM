from pyspark.sql import functions as F

prepared_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("event_at") >= F.lit("2025-01-13 14:00:00").cast("timestamp")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        (F.col("event_at") + F.expr("INTERVAL 7 DAYS")).alias("event_at_plus_7d"),
    )
)

exploded_detail_events = (
    prepared_events.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

qualified = (
    exploded_detail_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isNotNull()), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

profiled = (
    qualified.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

event_stats_2 = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

summarized_rows = (
    profiled.alias("s")
    .join(event_stats_2.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

annotated = (
    summarized_rows
    .filter(F.col("event_event_type").isNotNull())
    .select(
        "id",
        "event_at",
        "actor",
        "event_id",
        "event_event_at",
        "event_actor",
        "event_event_type",
        "last_event_at",
        "first_event_at",
        "first_event_at_2",
        F.concat(F.col("event_event_type"), F.lit("#"), F.col("ticket_id").cast("string")).alias("event_event_type_ref"),
        F.dayofweek(F.col("event_at")).alias("event_at_dayofweek"),
    )
)

result = (
    annotated
    .select(
        "id",
        "actor",
        "event_actor",
        "event_event_type",
        "first_event_at_2",
        "event_event_type_ref",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
        F.coalesce(F.col("event_at_dayofweek"), F.lit(0)).alias("event_at_dayofweek_filled"),
    )
)
