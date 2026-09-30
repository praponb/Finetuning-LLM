from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

derived = (
    scoped
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.date_trunc("hour", F.col("event_at")).alias("event_at_hour"),
    )
)

exploded_detail = (
    derived.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_slug",
        "s.event_at_hour",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.event_type").alias("event_event_type"),
        F.col("t.actor").alias("event_actor"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["event_event_type", "event_type"], ["event_event_type"], ["event_type"]],
        "event_event_type", "event_type",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("event_type")).alias("event_type_list"),
        F.round(100 * F.count(F.when(F.col("event_actor") == "system", 1)) / F.count("*"), 1).alias("pct_event_actor_system"),
    )
)
