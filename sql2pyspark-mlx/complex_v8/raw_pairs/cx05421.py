from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_event_id_event_actor_by_even = Window.partitionBy("event_id", "event_actor").orderBy("event_at")
w_event_id_event_actor_by_even_2 = Window.partitionBy("event_id", "event_actor").orderBy("event_at", "id", "event_id")
w_event_id_event_actor = Window.partitionBy("event_id", "event_actor")

scoped_tickets = (
    df_tickets
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "priority",
        "csat",
        "tags",
        F.date_trunc("day", F.col("opened_at")).alias("opened_at_day"),
        F.greatest(F.col("csat"), F.col("requester_id")).alias("greatest_csat_requester_id"),
    )
)

joined_agent = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(~F.col("su.tier").isin("T3", "T2"))
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        "s.opened_at_day",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

event_stats = (
    df_ticket_events
    .groupBy("ticket_id")
    .agg(
        F.min("event_at").alias("first_event_at"),
        F.min("event_at").alias("first_event_at_2"),
        F.max("event_at").alias("last_event_at"),
    )
)

with_stats = (
    joined_agent.alias("s")
    .join(event_stats.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "t.first_event_at",
        "t.first_event_at_2",
        "t.last_event_at",
    )
)

enriched = (
    with_stats
    .filter(F.col("agent_id") <= 5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "agent_tier",
        "first_event_at",
        "last_event_at",
        F.lower(F.regexp_replace(F.trim(F.col("agent_tier")), " ", "_")).alias("agent_tier_slug"),
    )
)

detail_rows_events = (
    enriched.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.first_event_at",
        "s.agent_tier_slug",
        F.col("t.id").alias("event_id"),
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
        "t.event_type",
    )
)

windowed = (
    detail_rows_events
    .select(
        "id",
        "requester_id",
        "agent_id",
        "first_event_at",
        "event_id",
        "event_actor",
        "event_type",
        F.round(F.percent_rank().over(w_event_id_event_actor_by_even), 3).alias("event_at_percent_rank"),
        F.row_number().over(w_event_id_event_actor_by_even_2).alias("seq_no"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_event_id_event_actor), 2).alias("requester_id_vs_event_id_event_actor_avg"),
    )
)

enriched_event = (
    windowed.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.id") == F.col("s.event_id"), "inner")
    .join(df_tickets.alias("ti"), F.col("ti.id") == F.col("t.ticket_id"), "left")
    .filter(F.col("t.ticket_id") >= 55)
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.first_event_at",
        "s.event_id",
        "s.event_type",
        "s.event_at_percent_rank",
        "s.seq_no",
        "s.requester_id_vs_event_id_event_actor_avg",
        F.col("t.ticket_id").alias("event_ticket_id"),
        "t.event_at",
        F.col("ti.opened_at").alias("ticket_opened_at"),
    )
)

result = (
    enriched_event
    .cube("event_ticket_id", "agent_id", "seq_no")
    .agg(
        F.round(F.sum("requester_id_vs_event_id_event_actor_avg"), 2).alias("total_requester_id_vs_event_id_event_actor_avg"),
    )
    .orderBy(
        F.col("event_ticket_id").asc_nulls_last(),
        F.col("agent_id").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
    )
)
