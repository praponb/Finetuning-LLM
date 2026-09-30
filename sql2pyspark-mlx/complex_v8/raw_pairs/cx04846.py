from pyspark.sql import functions as F

scoped = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) & (F.col("actor") == "customer"))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.lower(F.regexp_replace(F.trim(F.col("event_type")), " ", "_")).alias("event_type_slug"),
        F.concat_ws("-", F.col("actor"), F.col("event_type")).alias("actor_event_type_label"),
    )
)

derived = (
    scoped
    .filter(F.col("event_at") >= F.lit("2025-01-25 09:00:00").cast("timestamp"))
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_event_type_label",
        F.substring(F.trim(F.col("actor_event_type_label")), 1, 4).alias("actor_event_type_label_prefix"),
        F.date_format(F.col("event_at"), "yyyy-MM-dd").alias("event_at_label"),
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
    )
)

qualified = (
    derived.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.col("t.opened_at") < F.lit("2025-02-24 10:00:00").cast("timestamp")), "left_semi")
)

with_details_ticket = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .filter(F.col("t.priority") == "P4")
    .select(
        "s.*",
        F.col("t.priority").alias("ticket_priority"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("su.team").alias("agent_team"),
    )
)

derived_2 = (
    with_details_ticket
    .filter(F.col("ticket_id") >= 17)
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_event_type_label",
        "actor_event_type_label_prefix",
        "event_at_label",
        "ticket_priority",
        "agent_team",
        F.substring(F.trim(F.col("actor")), 1, 4).alias("actor_prefix"),
        F.to_date(F.col("ticket_closed_at")).alias("ticket_closed_at_date"),
        F.date_trunc("month", F.col("ticket_closed_at")).alias("ticket_closed_at_month"),
    )
)

rollup_base = (
    derived_2
    .filter(F.col("ticket_closed_at_date").between(F.lit("2024-09-01").cast("date"), F.lit("2025-04-01").cast("date")))
    .groupBy("actor", "ticket_priority")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("ticket_id").alias("distinct_ticket_ids"),
    )
    .filter(F.col("row_count") >= 2)
)

result = (
    rollup_base
    .groupingSets(
        [["row_count", "ticket_priority", "actor"], ["row_count"], ["ticket_priority"], []],
        "row_count", "ticket_priority", "actor",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.sum("distinct_ticket_ids").alias("total_distinct_ticket_ids"),
        F.array_sort(F.collect_set("ticket_priority")).alias("ticket_priority_list"),
    )
    .orderBy(
        F.col("row_count").asc_nulls_last(),
        F.col("ticket_priority").asc_nulls_last(),
        F.col("actor").asc_nulls_last(),
    )
)
