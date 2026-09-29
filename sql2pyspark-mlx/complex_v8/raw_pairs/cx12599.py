from pyspark.sql import functions as F

valid_events = (
    df_ticket_events
    .filter((F.col("actor") == "system") & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
    )
)

tagged = (
    valid_events
    .filter(F.col("actor") == "system")
    .select(
        "*",
        F.to_date(F.col("event_at")).alias("event_at_date"),
        F.when(F.col("actor").isin("agent", "system"), "group_a").when(F.col("actor") == "customer", "group_b").otherwise("other").alias("actor_group"),
        F.lower(F.regexp_replace(F.trim(F.col("actor")), " ", "_")).alias("actor_slug"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
    )
)

summarized_rows = (
    tagged.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.first_event_at", "t.first_event_at_2")
)

qualified = (
    summarized_rows.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type").isin("created", "replied")) & (~F.col("t.actor").isin("system", "agent")), "left_semi")
    .filter(F.col("event_type").isNotNull())
)

result = (
    qualified
    .select(
        "id",
        "ticket_id",
        "event_at",
        "actor",
        "actor_slug",
        F.length(F.trim(F.col("actor_slug"))).alias("actor_slug_len"),
        F.length(F.trim(F.col("actor_group"))).alias("actor_group_len"),
    )
    .orderBy("id")
)
