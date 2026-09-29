from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_team_csat = Window.partitionBy("agent_team", "csat")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_all_by_csat_ties = Window.orderBy(F.col("csat").desc())
w_all_by_csat_running = Window.orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped_tickets = (
    df_tickets
    .filter(F.col("closed_at") < F.lit("2025-02-22 12:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
    )
)

joined = (
    scoped_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.opened_at",
        "s.channel",
        "s.csat",
        "s.priority_prefix",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

annotated = (
    joined
    .select(
        "id",
        "agent_id",
        "opened_at",
        "channel",
        "csat",
        "agent_tier",
        "agent_team",
        F.initcap(F.trim(F.col("agent_team"))).alias("agent_team_clean"),
    )
)

metrics = (
    annotated
    .select(
        "*",
        F.min("csat").over(w_agent_team_csat).alias("agent_team_csat_min_csat"),
        F.row_number().over(w_all_by_csat).alias("rn"),
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_csat), 3).alias("csat_percent_rank_2"),
        F.last("opened_at", ignorenulls=True).over(w_all_by_csat_running).alias("last_known_opened_at"),
    )
)

result = (
    metrics
    .rollup("agent_tier", "csat")
    .agg(F.min("last_known_opened_at").alias("first_last_known_opened_at"))
    .select(
        F.coalesce(F.col("agent_tier"), F.lit("ALL")).alias("agent_tier"),
        "csat",
        "first_last_known_opened_at",
    )
)
