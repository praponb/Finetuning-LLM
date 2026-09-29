from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_team_or_unknown = Window.partitionBy("team_or_unknown")
w_tier_max_ticket_csat_by_max = Window.partitionBy("tier", "max_ticket_csat").orderBy(F.col("max_ticket_csat").asc_nulls_last())

base_agents = (
    df_support_agents
    .filter((F.year(F.col("hired_on")) == 2025) & (F.col("agent_name").isNotNull()))
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        F.coalesce(F.col("team"), F.lit("UNKNOWN")).alias("team_or_unknown"),
    )
)

ticket_stats = (
    df_tickets
    .groupBy("agent_id")
    .agg(F.max("csat").alias("max_ticket_csat"))
)

profiled = (
    base_agents.alias("s")
    .join(ticket_stats.alias("t"), F.col("t.agent_id") == F.col("s.id"), "left")
    .select("s.*", "t.max_ticket_csat")
)

windowed = (
    profiled
    .select(
        "id",
        "team",
        "tier",
        "team_or_unknown",
        "max_ticket_csat",
        F.round(F.col("max_ticket_csat") / F.nullif(F.sum("max_ticket_csat").over(w_team_or_unknown), F.lit(0)), 4).alias("max_ticket_csat_share_of_team_or_unknown"),
        F.rank().over(w_tier_max_ticket_csat_by_max).alias("max_ticket_csat_rank"),
        F.dense_rank().over(w_tier_max_ticket_csat_by_max).alias("max_ticket_csat_dense_rank"),
    )
)

result = (
    windowed
    .filter(F.col("max_ticket_csat_share_of_team_or_unknown") > 0.1)
    .select(
        "id",
        "max_ticket_csat",
        "max_ticket_csat_share_of_team_or_unknown",
        "max_ticket_csat_dense_rank",
    )
    .orderBy("max_ticket_csat_dense_rank", "id")
    .limit(20)
)
