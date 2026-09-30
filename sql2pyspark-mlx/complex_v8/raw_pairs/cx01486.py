from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_by_requester_id = Window.partitionBy("priority").orderBy("requester_id", "id")
w_agent_id_channel_by_csat = Window.partitionBy("agent_id", "channel").orderBy("csat", "id")

cleaned_tickets = (
    df_tickets
    .filter(F.col("priority").isNotNull())
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat(F.col("channel"), F.lit("#"), F.col("requester_id").cast("string")).alias("channel_ref"),
    )
)

sequenced = (
    cleaned_tickets
    .select(
        "*",
        F.rank().over(w_priority_by_requester_id).alias("requester_id_rank"),
        F.lead("closed_at", 3).over(w_agent_id_channel_by_csat).alias("next3_closed_at"),
        (F.col("csat") - F.lag("csat").over(w_agent_id_channel_by_csat)).alias("csat_change"),
        F.lead("opened_at").over(w_agent_id_channel_by_csat).alias("next_opened_at"),
    )
)

flattened = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "channel_ref",
        "requester_id_rank",
        "next3_closed_at",
        "csat_change",
        "next_opened_at",
        F.explode("tags").alias("tag"),
    )
)

with_parent_agent = (
    flattened.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.priority",
        "s.csat",
        "s.channel_ref",
        "s.requester_id_rank",
        "s.csat_change",
        "s.next_opened_at",
        "s.tag",
        "su.agent_name",
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority") == "P2", 1)).alias("ticket_priority_count"),
        F.max("csat").alias("max_ticket_csat"),
    )
)

with_stats = (
    with_parent_agent.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
        "t.max_ticket_csat",
    )
)

with_activity = (
    with_stats.alias("s")
    .join(df_ticket_events.alias("t"), (F.col("t.ticket_id") == F.col("s.id")) & (F.col("t.actor") == "agent"), "left_anti")
)

summary = (
    with_activity
    .groupBy("agent_id")
    .agg(
        F.round(F.avg("requester_id_rank"), 2).alias("avg_requester_id_rank"),
        F.sum(F.when(F.length(F.trim(F.col("agent_name"))) >= 8, F.col("requester_id")).otherwise(0)).alias("requester_id_length_trim_agent_name_8"),
        F.min("ticket_priority_count").alias("min_ticket_priority_count"),
        F.min("closed_at").alias("first_closed_at"),
        F.max("closed_at").alias("last_closed_at"),
        F.count("*").alias("row_count"),
    )
)

result = (
    summary
    .select(
        "agent_id",
        "requester_id_length_trim_agent_name_8",
        "row_count",
        "min_ticket_priority_count",
    )
    .unpivot(
        ids=["agent_id"],
        values=["requester_id_length_trim_agent_name_8", "row_count", "min_ticket_priority_count"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
