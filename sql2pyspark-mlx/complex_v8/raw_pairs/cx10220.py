from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_priority_agent_id = Window.partitionBy("priority", "agent_id")
w_all_by_closed_at_moving_2 = Window.orderBy("closed_at", "id").rowsBetween(-2, Window.currentRow)
w_channel_csat_by_requester_id = Window.partitionBy("channel", "csat").orderBy("requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

base_tickets = df_tickets.filter(~F.col("channel").isin("phone", "chat"))

metrics = (
    base_tickets
    .select(
        "*",
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_priority_agent_id), 2).alias("requester_id_vs_priority_agent_id_avg"),
        F.round(F.avg("csat").over(w_all_by_closed_at_moving_2), 2).alias("csat_avg_3"),
        F.last("closed_at").over(w_channel_csat_by_requester_id).alias("last_closed_at"),
    )
)

agent_id_stats = (
    metrics
    .groupBy("agent_id")
    .agg(F.avg("requester_id").alias("avg_requester_id"))
)

outliers = (
    metrics.alias("s")
    .join(agent_id_stats.alias("g"), F.col("g.agent_id") == F.col("s.agent_id"), "inner")
    .filter(F.col("s.requester_id") > F.col("g.avg_requester_id"))
    .select(
        "s.*",
        "g.avg_requester_id",
        F.round(F.col("s.requester_id") - F.col("g.avg_requester_id"), 2).alias("requester_id_minus_avg"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(
        F.count(F.when(F.col("priority") != "P4", 1)).alias("ticket_priority_count"),
    )
)

profiled = (
    outliers.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("t.ticket_priority_count"), F.lit(0)).alias("ticket_priority_count"),
    )
)

combined_details_agent = (
    profiled.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.csat_avg_3",
        "s.last_closed_at",
        "s.avg_requester_id",
        "s.requester_id_minus_avg",
        "s.ticket_priority_count",
        "su.agent_name",
    )
)

result = (
    combined_details_agent
    .groupBy("agent_id")
    .pivot("channel", ["email", "web", "phone"])
    .agg(F.max("avg_requester_id"))
    .orderBy("agent_id")
)
