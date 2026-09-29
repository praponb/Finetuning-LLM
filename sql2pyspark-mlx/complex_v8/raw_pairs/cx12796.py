from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_by_csat = Window.partitionBy("agent_tier").orderBy("csat", "id")
w_csat_by_csat = Window.partitionBy("csat").orderBy(F.col("csat").desc(), "id")
w_csat_by_csat_full = Window.partitionBy("csat").orderBy(F.col("csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_by_csat_prior_3 = Window.partitionBy("csat").orderBy(F.col("csat").desc(), "id").rowsBetween(-3, -1)
w_csat = Window.partitionBy("csat")
w_agent_id_csat = Window.partitionBy("agent_id", "csat")
w_channel_len_priority_by_chan = Window.partitionBy("channel_len", "priority").orderBy("channel_len")
w_csat_priority_by_channel_len = Window.partitionBy("csat", "priority").orderBy(F.col("channel_len_percent_rank").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_len = Window.partitionBy("channel_len")
w_all_by_prev_csat_full = Window.orderBy(F.col("prev_csat").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped = (
    df_tickets
    .filter((F.array_contains(F.col("tags"), "login")) & (~F.col("channel").isin("email", "phone")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "csat",
        "tags",
        F.concat(F.col("priority"), F.lit("#"), F.col("id").cast("string")).alias("priority_ref"),
        F.length(F.trim(F.col("channel"))).alias("channel_len"),
    )
)

enriched = (
    scoped.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.priority",
        "s.csat",
        "s.tags",
        "s.channel_len",
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_trends = (
    enriched
    .select(
        "*",
        F.lag("csat").over(w_agent_tier_by_csat).alias("prev_csat"),
        (F.col("channel_len") - F.lag("channel_len").over(w_csat_by_csat)).alias("channel_len_change"),
        F.nth_value("csat", 2).over(w_csat_by_csat_full).alias("second_csat"),
        (F.col("channel_len") - F.lag("channel_len").over(w_agent_tier_by_csat)).alias("channel_len_change_2"),
        F.round(F.avg("csat").over(w_csat_by_csat_prior_3), 2).alias("csat_avg_3_prior"),
        F.size(F.collect_set("priority").over(w_csat)).alias("distinct_priority_in_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id_csat), 2).alias("csat_vs_agent_id_csat_avg"),
        F.round(F.percent_rank().over(w_channel_len_priority_by_chan), 3).alias("channel_len_percent_rank"),
        F.lead("channel_len").over(w_csat_by_csat).alias("next_channel_len"),
    )
)

metrics = (
    with_trends
    .select(
        "*",
        F.nth_value("channel_len_percent_rank", 2).over(w_csat_priority_by_channel_len).alias("second_channel_len_percent_rank"),
        F.round(F.col("channel_len_change_2") - F.avg("channel_len_change_2").over(w_channel_len), 2).alias("channel_len_change_2_vs_channel_len_avg"),
        F.last("csat_vs_agent_id_csat_avg").over(w_all_by_prev_csat_full).alias("last_csat_vs_agent_id_csat_avg"),
    )
)

result = (
    metrics
    .select(
        "id",
        "agent_id",
        "priority",
        "tags",
        "channel_len",
        "agent_tier",
        "agent_name",
        "channel_len_change",
        "channel_len_change_2",
        "csat_avg_3_prior",
        "distinct_priority_in_csat",
        "csat_vs_agent_id_csat_avg",
        "channel_len_percent_rank",
        "next_channel_len",
        "second_channel_len_percent_rank",
        "channel_len_change_2_vs_channel_len_avg",
        "last_csat_vs_agent_id_csat_avg",
    )
    .orderBy(F.col("channel_len_change_2_vs_channel_len_avg").asc_nulls_last())
)
