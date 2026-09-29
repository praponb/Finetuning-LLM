from pyspark.sql import functions as F

scoped_events = (
    df_ticket_events
    .filter(F.month(F.col("event_at")).isin(1, 2))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
    )
)

screened = (
    scoped_events.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.actor").isin("agent", "customer")), "left_semi")
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(F.max("event_at").alias("last_event_at"))
)

profiled = (
    screened.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.ticket_id"), "left")
    .select("s.*", "t.last_event_at")
)

with_parent_ticket = (
    profiled.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.agent_id").alias("ticket_agent_id"),
        F.col("su.tier").alias("agent_tier"),
    )
)

detail_rows_events = (
    with_parent_ticket.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.col("t.event_type") == "escalated"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.last_event_at",
        "s.ticket_agent_id",
        F.col("t.id").alias("event_id"),
        F.col("t.event_at").alias("event_event_at"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

grouped = (
    detail_rows_events
    .filter(F.col("event_id").isNotNull())
    .groupBy("event_actor")
    .agg(
        F.array_sort(F.collect_set("event_event_type")).alias("event_event_type_list"),
        F.countDistinct("event_id").alias("distinct_event_ids"),
    )
)

tagged = (
    grouped
    .select(
        "event_actor",
        "event_event_type_list",
        F.concat(F.col("event_actor"), F.lit("#"), F.col("distinct_event_ids").cast("string")).alias("event_actor_ref"),
        F.coalesce(F.col("distinct_event_ids"), F.lit(0)).alias("distinct_event_ids_filled"),
        F.coalesce(F.col("distinct_event_ids"), F.lit(0)).alias("distinct_event_ids_filled_2"),
    )
)

result = (
    tagged
    .groupingSets(
        [["event_actor", "distinct_event_ids_filled"], ["event_actor"], ["distinct_event_ids_filled"], []],
        "event_actor", "distinct_event_ids_filled",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.round(F.sum("distinct_event_ids_filled_2") / F.nullif(F.sum("distinct_event_ids_filled"), F.lit(0)), 3).alias("distinct_event_ids_filled_2_to_distinct_event_id"),
        F.count(F.when(F.col("event_actor") == "system", 1)).alias("n_event_actor_system"),
        F.round(F.sum("distinct_event_ids_filled") / F.nullif(F.sum("distinct_event_ids_filled_2"), F.lit(0)), 3).alias("distinct_event_ids_filled_to_distinct_event_ids_"),
    )
    .select(
        F.coalesce(F.col("event_actor"), F.lit("ALL")).alias("event_actor"),
        "distinct_event_ids_filled",
        "grouping_level",
        "distinct_event_ids_filled_2_to_distinct_event_id",
        "n_event_actor_system",
        "distinct_event_ids_filled_to_distinct_event_ids_",
    )
    .orderBy(
        F.col("event_actor").asc_nulls_last(),
        F.col("distinct_event_ids_filled").asc_nulls_last(),
    )
)
