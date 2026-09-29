from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_priority_or_unknown_by = Window.partitionBy("csat", "priority_or_unknown").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_priority_agent_team = Window.partitionBy("priority", "agent_team")
w_csat_priority_or_unknown_by_2 = Window.partitionBy("csat", "priority_or_unknown").orderBy("csat", "id")
w_all_by_agent_hired_on_ties = Window.orderBy("agent_hired_on")

recent = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(6, 8)) & (F.col("closed_at") < F.lit("2025-02-27 06:00:00").cast("timestamp")))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.coalesce(F.col("priority"), F.lit("UNKNOWN")).alias("priority_or_unknown"),
        F.concat(F.col("priority"), F.lit("#"), F.col("csat").cast("string")).alias("priority_ref"),
    )
)

enriched = (
    recent.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_or_unknown",
        "s.priority_ref",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

enriched_2 = (
    enriched
    .filter(~F.array_contains(F.col("tags"), "refund"))
    .select(
        "*",
        F.date_format(F.col("closed_at"), "yyyy-MM").alias("closed_at_label"),
    )
)

with_windows = (
    enriched_2
    .select(
        "id",
        "agent_id",
        "priority",
        "csat",
        "tags",
        "priority_or_unknown",
        "priority_ref",
        "agent_tier",
        "agent_team",
        "closed_at_label",
        F.last("csat").over(w_csat_priority_or_unknown_by).alias("last_csat"),
        F.max("csat").over(w_priority_agent_team).alias("priority_agent_team_max_csat"),
        F.ntile(4).over(w_csat_priority_or_unknown_by_2).alias("csat_quartile"),
        F.lead("closed_at").over(w_csat_priority_or_unknown_by_2).alias("next_closed_at"),
        F.count(F.lit(1)).over(w_priority_agent_team).alias("rows_in_priority_agent_team"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on_ties), 3).alias("agent_hired_on_cume_dist"),
    )
)

result = (
    with_windows
    .rollup("priority", "csat")
    .agg(
        F.grouping("priority").alias("is_total_priority"),
        F.max("agent_hired_on_cume_dist").alias("max_agent_hired_on_cume_dist"),
        F.sum("priority_agent_team_max_csat").alias("total_priority_agent_team_max_csat"),
    )
    .orderBy(F.col("priority").asc_nulls_last(), F.col("csat").asc_nulls_last())
)
