from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_csat_by_opened_at_mo = Window.partitionBy("channel", "csat").orderBy("opened_at", "id").rowsBetween(-2, Window.currentRow)
w_agent_team = Window.partitionBy("agent_team")
w_agent_id_by_csat_running = Window.partitionBy("agent_id").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = df_tickets.filter((F.col("priority") != "P2") & (F.hour(F.col("closed_at")).between(6, 11)))

combined_details_agent = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T3", "T2")), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    combined_details_agent
    .select(
        "*",
        F.round(F.avg("csat").over(w_channel_csat_by_opened_at_mo), 2).alias("csat_avg_3"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_agent_team), F.lit(0)), 4).alias("requester_id_share_of_agent_team"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_team), F.lit(0)), 4).alias("csat_share_of_agent_team"),
        F.first("requester_id", ignorenulls=True).over(w_agent_id_by_csat_running).alias("first_known_requester_id"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "opened_at",
        "channel",
        "csat",
        "agent_team",
        "requester_id_share_of_agent_team",
        "csat_share_of_agent_team",
        "first_known_requester_id",
        F.abs(F.col("csat_share_of_agent_team") - 0.6).alias("csat_share_of_agent_team_distance"),
        F.round(F.col("first_known_requester_id") / F.nullif(F.col("csat"), F.lit(0)), 3).alias("first_known_requester_id_per_csat"),
    )
    .orderBy(F.col("agent_team").desc())
)
