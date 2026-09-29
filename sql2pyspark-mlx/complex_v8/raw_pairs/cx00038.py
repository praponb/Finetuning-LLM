from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id_opened_at_to_date_b = Window.partitionBy("agent_id", F.to_date("opened_at")).orderBy("agent_hired_on", "id", "event_id")
w_all_by_last_ticket_opened_at = Window.orderBy(F.col("last_ticket_opened_at").desc())
w_all_by_last_ticket_opened_at_2 = Window.orderBy(F.col("last_ticket_opened_at").desc(), "id", "event_id")

valid = (
    df_tickets
    .filter((F.col("priority").isin("P2", "P1", "P4")) & (F.col("requester_id") < 18))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

exploded_detail = (
    valid.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.csat",
        F.col("t.id").alias("event_id"),
        "t.event_type",
        F.col("t.actor").alias("event_actor"),
        "t.event_at",
    )
)

with_parent_agent = (
    exploded_detail.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.priority",
        "s.event_id",
        "s.event_type",
        "s.event_at",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.min("closed_at").alias("first_ticket_closed_at"),
        F.max("opened_at").alias("last_ticket_opened_at"),
    )
)

with_stats = (
    with_parent_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select("s.*", "t.first_ticket_closed_at", "t.last_ticket_opened_at")
)

sequenced = (
    with_stats
    .select(
        "*",
        F.ntile(10).over(w_agent_id_opened_at_to_date_b).alias("agent_hired_on_decile"),
        F.round(F.cume_dist().over(w_all_by_last_ticket_opened_at), 3).alias("last_ticket_opened_at_cume_dist"),
        F.round(F.cume_dist().over(w_all_by_last_ticket_opened_at_2), 3).alias("last_ticket_opened_at_cume_dist_2"),
        F.row_number().over(w_agent_id_opened_at_to_date_b).alias("seq_no"),
    )
)

with_children = (
    sequenced.alias("s")
    .join(df_ticket_events.alias("t"), F.col("t.ticket_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.event_id",
        "s.event_type",
        "s.event_at",
        "s.first_ticket_closed_at",
        "s.last_ticket_opened_at",
        "s.agent_hired_on_decile",
        "s.last_ticket_opened_at_cume_dist",
        "s.last_ticket_opened_at_cume_dist_2",
        F.col("t.id").alias("event_id_2"),
        F.col("t.actor").alias("event_actor"),
        F.col("t.event_type").alias("event_event_type"),
    )
)

screened = (
    with_children.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.event_type") == "escalated"), "left_anti")
)

result = (
    screened
    .groupingSets(
        [["agent_hired_on_decile", "event_id_2"], ["agent_hired_on_decile"], ["event_id_2"]],
        "agent_hired_on_decile", "event_id_2",
    )
    .agg(
        F.sum(F.when(F.col("event_type").isin("reopened", "resolved"), F.col("last_ticket_opened_at_cume_dist")).otherwise(0)).alias("last_ticket_opened_at_cume_dist_event_type_in_re"),
        F.sum(F.when(F.col("event_actor").isin("customer", "system"), F.col("agent_hired_on_decile")).otherwise(0)).alias("agent_hired_on_decile_event_actor_in_customer_sy"),
        F.round(100 * F.count(F.when(F.col("event_actor") == "agent", 1)) / F.count("*"), 1).alias("pct_event_actor_agent"),
    )
    .orderBy(
        F.col("agent_hired_on_decile").asc_nulls_last(),
        F.col("event_id_2").asc_nulls_last(),
    )
)
