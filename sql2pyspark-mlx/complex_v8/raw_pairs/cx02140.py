from pyspark.sql import functions as F

base_events = (
    df_ticket_events
    .filter((F.month(F.col("event_at")).isin(1, 2)) & (F.col("ticket_id").isNotNull()))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.when(F.col("event_type").isin("resolved", "replied"), "group_a").when(F.col("event_type") == "reopened", "group_b").otherwise("other").alias("event_type_group"),
        F.year(F.col("event_at")).alias("event_at_year"),
        F.substring(F.trim(F.col("actor")), 1, 3).alias("actor_prefix"),
    )
)

event_type_group_stats = (
    base_events
    .groupBy("event_type_group")
    .agg(F.avg("event_at_year").alias("avg_event_at_year"))
)

compared = (
    base_events.alias("s")
    .join(event_type_group_stats.alias("g"), F.col("g.event_type_group") == F.col("s.event_type_group"), "inner")
    .filter(F.col("s.event_at_year") > F.col("g.avg_event_at_year"))
    .select(
        "s.*",
        "g.avg_event_at_year",
        F.round(F.col("s.event_at_year") - F.col("g.avg_event_at_year"), 2).alias("event_at_year_minus_avg"),
    )
)

joined = (
    compared.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "inner")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_type",
        "s.event_at_year",
        "s.actor_prefix",
        "s.avg_event_at_year",
        F.col("t.requester_id").alias("ticket_requester_id"),
        F.col("t.closed_at").alias("ticket_closed_at"),
        F.col("t.opened_at").alias("ticket_opened_at"),
        "su.agent_name",
    )
)

result = (
    joined
    .groupingSets(
        [["event_at_year", "ticket_id", "event_type"], ["event_at_year"], ["ticket_id"]],
        "event_at_year", "ticket_id", "event_type",
    )
    .agg(
        F.min("ticket_closed_at").alias("first_ticket_closed_at"),
        F.sum("ticket_requester_id").alias("total_ticket_requester_id"),
    )
    .orderBy(
        F.col("event_at_year").asc_nulls_last(),
        F.col("ticket_id").asc_nulls_last(),
        F.col("event_type").asc_nulls_last(),
    )
)
