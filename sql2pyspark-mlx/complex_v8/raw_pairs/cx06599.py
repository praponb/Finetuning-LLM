from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ticket_channel_by_ticket_ope = Window.partitionBy("ticket_channel").orderBy(F.col("ticket_opened_at_2").desc())
w_ticket_channel_by_ticket_ope_2 = Window.partitionBy("ticket_channel").orderBy(F.col("ticket_opened_at_2").desc(), "id")
w_actor_group_ticket_agent_id = Window.partitionBy("actor_group", "ticket_agent_id").orderBy(F.col("ticket_opened_at_2_cume_dist").desc())
w_actor_group_ticket_agent_id_2 = Window.partitionBy("actor_group", "ticket_agent_id").orderBy(F.col("ticket_opened_at_2_cume_dist").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team = Window.partitionBy("agent_team")

cleaned_events = (
    df_ticket_events
    .filter((F.col("actor") == "agent") & (F.col("event_type").isin("created", "replied")))
    .select(
        "id",
        "ticket_id",
        "event_at",
        "event_type",
        "actor",
        F.upper(F.trim(F.col("actor"))).alias("actor_clean"),
        F.when(F.col("actor").isin("customer", "agent"), "group_a").when(F.col("actor") == "system", "group_b").otherwise("other").alias("actor_group"),
    )
)

with_details = (
    cleaned_events.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.event_at",
        "s.event_type",
        "s.actor",
        "s.actor_clean",
        "s.actor_group",
        F.col("t.opened_at").alias("ticket_opened_at"),
        F.col("t.channel").alias("ticket_channel"),
        F.col("su.tier").alias("agent_tier"),
    )
)

qualified = (
    with_details.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.ticket_id")) & (F.to_date(F.col("t.event_at")) < F.lit("2025-02-12").cast("date")), "left_semi")
    .filter(F.col("actor_clean").isNotNull())
)

enriched = (
    qualified.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.id") == F.col("s.ticket_id")) & (F.array_contains(F.col("t.tags"), "refund")), "inner")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("t.agent_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_group",
        "s.ticket_opened_at",
        "s.ticket_channel",
        "s.agent_tier",
        F.col("t.opened_at").alias("ticket_opened_at_2"),
        F.col("t.priority").alias("ticket_priority"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier_2"),
    )
)

enriched_2 = (
    enriched.alias("s")
    .join(df_tickets.alias("t"), F.col("t.id") == F.col("s.ticket_id"), "left")
    .select(
        "s.id",
        "s.ticket_id",
        "s.actor",
        "s.actor_group",
        "s.ticket_opened_at",
        "s.ticket_channel",
        "s.agent_tier",
        "s.ticket_opened_at_2",
        "s.ticket_priority",
        "s.agent_team",
        F.col("t.agent_id").alias("ticket_agent_id"),
    )
)

with_windows = (
    enriched_2
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_group",
        "agent_tier",
        "ticket_opened_at_2",
        "ticket_priority",
        "agent_team",
        "ticket_agent_id",
        F.round(F.cume_dist().over(w_ticket_channel_by_ticket_ope), 3).alias("ticket_opened_at_2_cume_dist"),
        F.lag("ticket_opened_at_2", 2).over(w_ticket_channel_by_ticket_ope_2).alias("prev2_ticket_opened_at_2"),
    )
)

sequenced = (
    with_windows
    .select(
        "id",
        "ticket_id",
        "actor",
        "actor_group",
        "agent_tier",
        "ticket_opened_at_2",
        "ticket_priority",
        "agent_team",
        F.round(F.percent_rank().over(w_actor_group_ticket_agent_id), 3).alias("ticket_opened_at_2_cume_dist_percent_rank"),
        F.sum("ticket_opened_at_2_cume_dist").over(w_actor_group_ticket_agent_id_2).alias("running_ticket_opened_at_2_cume_dist"),
        F.size(F.collect_set("ticket_agent_id").over(w_agent_team)).alias("distinct_ticket_agent_id_in_agent_team"),
    )
)

result = (
    sequenced
    .filter(F.col("ticket_opened_at_2_cume_dist_percent_rank").between(0.1, 0.7))
    .select(
        "id",
        "actor_group",
        "agent_tier",
        "ticket_opened_at_2",
        "ticket_priority",
        "ticket_opened_at_2_cume_dist_percent_rank",
        "distinct_ticket_agent_id_in_agent_team",
    )
    .orderBy(F.col("agent_tier").asc_nulls_last(), "id")
    .limit(20)
)
