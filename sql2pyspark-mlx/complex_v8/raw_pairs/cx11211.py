from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_min_csat_distinct_agent_tier = Window.partitionBy("min_csat", "distinct_agent_tiers")
w_priority_list_item_by_min_cs = Window.partitionBy("priority_list_item").orderBy("min_csat")
w_csat_agent_tier_by_avg_csat = Window.partitionBy("csat", "agent_tier").orderBy("avg_csat")

filtered = (
    df_tickets
    .filter(F.col("closed_at") >= F.lit("2025-01-17 21:00:00").cast("timestamp"))
    .select(
        "id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.concat_ws("-", F.col("channel"), F.col("priority")).alias("channel_priority_label"),
        (F.floor(F.col("requester_id") / 5) * 5).alias("requester_id_bucket"),
    )
)

joined = (
    filtered.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .filter(F.length(F.trim(F.col("su.agent_name"))) >= 7)
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.channel_priority_label",
        "s.requester_id_bucket",
        "su.agent_name",
        F.col("su.tier").alias("agent_tier"),
    )
)

totals = (
    joined
    .groupBy("agent_tier", "agent_id", "csat")
    .agg(
        F.round(F.sum("csat") / F.nullif(F.sum("requester_id_bucket"), F.lit(0)), 3).alias("csat_to_requester_id_bucket_ratio"),
        F.array_sort(F.collect_set("priority")).alias("priority_list"),
        F.sum("requester_id_bucket").alias("total_requester_id_bucket"),
        F.round(F.avg("csat"), 2).alias("avg_csat"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
        F.min("csat").alias("min_csat"),
    )
)

with_activity = (
    totals.alias("s")
    .join(df_tickets.alias("t"), (F.col("t.agent_id") == F.col("s.agent_id")) & (F.to_date(F.col("t.opened_at")) < F.lit("2025-02-12").cast("date")) & (F.col("t.requester_id").between(2, 23)), "left_semi")
)

flattened = (
    with_activity
    .select(
        "agent_tier",
        "agent_id",
        "csat",
        "csat_to_requester_id_bucket_ratio",
        "total_requester_id_bucket",
        "avg_csat",
        "distinct_agent_tiers",
        "min_csat",
        F.explode("priority_list").alias("priority_list_item"),
    )
)

with_windows = (
    flattened
    .select(
        "agent_tier",
        "agent_id",
        "csat",
        "total_requester_id_bucket",
        "avg_csat",
        "distinct_agent_tiers",
        "min_csat",
        "priority_list_item",
        F.min("min_csat").over(w_min_csat_distinct_agent_tier).alias("min_csat_distinct_agent_tiers_min_min_csat"),
        F.dense_rank().over(w_priority_list_item_by_min_cs).alias("min_csat_dense_rank"),
        F.round(F.cume_dist().over(w_csat_agent_tier_by_avg_csat), 3).alias("avg_csat_cume_dist"),
        F.min("avg_csat").over(w_min_csat_distinct_agent_tier).alias("min_csat_distinct_agent_tiers_min_avg_csat"),
        F.round(F.col("min_csat") - F.avg("min_csat").over(w_min_csat_distinct_agent_tier), 2).alias("min_csat_vs_min_csat_distinct_agent_tiers_avg"),
        F.size(F.collect_set("agent_tier").over(w_min_csat_distinct_agent_tier)).alias("distinct_agent_tier_in_min_csat_distinct_agent_t"),
    )
)

result = (
    with_windows
    .groupBy("distinct_agent_tier_in_min_csat_distinct_agent_t")
    .pivot("agent_tier", ["T1", "T3"])
    .agg(F.avg("min_csat_distinct_agent_tiers_min_avg_csat"))
)
