from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_2_by_csat_running = Window.partitionBy("agent_tier_2").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_priority = Window.partitionBy("agent_tier", "priority")
w_agent_tier_2_by_csat = Window.partitionBy("agent_tier_2").orderBy("csat", "id")
w_all_by_csat_running = Window.orderBy(F.col("csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

scoped = (
    df_tickets
    .filter(F.col("channel").isin("phone", "web"))
    .select(
        "id",
        "agent_id",
        "priority",
        "channel",
        "csat",
    )
)

combined_details_agent = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.tier").isin("T3", "T2")), "inner")
    .filter(F.upper(F.trim(F.col("su.agent_name"))).like("F%"))
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.channel",
        "s.csat",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

combined_details = (
    combined_details_agent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.*",
        F.col("su.tier").alias("agent_tier_2"),
        F.col("su.agent_name").alias("agent_agent_name"),
    )
)

ranked = (
    combined_details
    .select(
        "*",
        F.last("csat", ignorenulls=True).over(w_agent_tier_2_by_csat_running).alias("last_known_csat"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_agent_tier_priority), F.lit(0)), 4).alias("csat_share_of_agent_tier_priority"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_tier_priority), 2).alias("csat_vs_agent_tier_priority_avg"),
        F.lag("csat").over(w_agent_tier_2_by_csat).alias("prev_csat"),
        F.sum("csat").over(w_all_by_csat_running).alias("running_csat"),
    )
)

result = (
    ranked
    .rollup("prev_csat", "last_known_csat")
    .agg(
        F.round(F.avg("csat_share_of_agent_tier_priority"), 2).alias("avg_csat_share_of_agent_tier_priority"),
    )
)
