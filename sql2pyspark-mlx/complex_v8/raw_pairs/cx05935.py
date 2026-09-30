from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_priority = Window.partitionBy("csat", "priority")
w_all_by_agent_hired_on = Window.orderBy(F.col("agent_hired_on").desc(), "id")
w_all_by_agent_hired_on_runnin = Window.orderBy(F.col("agent_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_csat = Window.orderBy("csat", "id")
w_all_by_csat_ties = Window.orderBy("csat")

scoped = (
    df_tickets
    .filter((F.hour(F.col("closed_at")).between(10, 13)) & (F.col("csat").between(1, 4)))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.substring(F.trim(F.col("priority")), 1, 2).alias("priority_prefix"),
        F.upper(F.trim(F.col("channel"))).alias("channel_clean"),
    )
)

with_details = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 8)
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        "s.priority_prefix",
        "s.channel_clean",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "agent_id",
        "closed_at",
        "channel",
        "csat",
        "tags",
        "agent_tier",
        "agent_hired_on",
        F.round(F.avg("csat").over(w_csat_priority), 2).alias("csat_priority_avg_csat"),
        F.count(F.lit(1)).over(w_csat_priority).alias("rows_in_csat_priority"),
        F.min("csat").over(w_csat_priority).alias("csat_priority_min_csat"),
        F.size(F.collect_set("agent_tier").over(w_csat_priority)).alias("distinct_agent_tier_in_csat_priority"),
        F.round(F.cume_dist().over(w_all_by_agent_hired_on), 3).alias("agent_hired_on_cume_dist"),
        F.count(F.lit(1)).over(w_all_by_agent_hired_on_runnin).alias("running_count"),
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_all_by_agent_hired_on)).alias("days_since_prev_agent_hired_on"),
        (F.col("csat") - F.lag("csat").over(w_all_by_agent_hired_on)).alias("csat_change"),
        F.size(F.collect_set("channel").over(w_csat_priority)).alias("distinct_channel_in_csat_priority"),
        F.rank().over(w_all_by_csat).alias("csat_rank"),
        F.dense_rank().over(w_all_by_csat_ties).alias("csat_dense_rank"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "closed_at",
        "channel",
        "tags",
        "agent_tier",
        "agent_hired_on",
        "csat_priority_avg_csat",
        "rows_in_csat_priority",
        "csat_priority_min_csat",
        "agent_hired_on_cume_dist",
        "running_count",
        "days_since_prev_agent_hired_on",
        "csat_change",
        "distinct_channel_in_csat_priority",
        "csat_rank",
        F.round(F.col("running_count") / F.nullif(F.col("csat_priority_avg_csat"), F.lit(0)), 3).alias("running_count_per_csat_priority_avg_csat"),
    )
)
