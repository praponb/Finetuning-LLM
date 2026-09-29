from pyspark.sql import functions as F

filtered_events = (
    df_ticket_events
    .filter(F.col("event_at") >= F.lit("2025-01-25 10:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.concat_ws(" / ", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
        F.when(F.col("event_type").isin("escalated", "resolved"), "group_a").when(F.col("event_type") == "assigned", "group_b").otherwise("other").alias("event_type_group"),
        F.date_format(F.col("event_at"), "yyyy-MM").alias("event_at_label"),
    )
)

with_activity = (
    filtered_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") != "assigned") & (F.hour(F.col("t.event_at")).between(8, 12)), "left_anti")
    .filter(F.hour(F.col("event_at")).between(9, 12))
)

enriched = (
    with_activity
    .select(
        "*",
        F.date_trunc("day", F.col("event_at")).alias("event_at_day"),
        F.concat(F.col("actor"), F.lit("#"), F.col("id").cast("string")).alias("actor_ref"),
    )
)

result = (
    enriched
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        "actor_event_type_label",
        "event_type_group",
        "event_at_label",
        "event_at_day",
        "actor_ref",
        F.when(F.col("event_type").isin("created", "replied"), "group_a").when(F.col("event_type") == "resolved", "group_b").otherwise("other").alias("event_type_group_2"),
        F.to_date(F.col("event_at")).alias("event_at_date"),
    )
    .orderBy(F.col("event_type_group_2").desc())
)
