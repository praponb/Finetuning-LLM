from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel = Window.partitionBy("channel")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_all_by_closed_at_prior_3 = Window.orderBy(F.col("closed_at").desc(), "id").rowsBetween(-3, -1)
w_agent_id_by_agent_hired_on_t = Window.partitionBy("agent_id").orderBy("agent_hired_on")
w_csat = Window.partitionBy("csat")
w_all_by_closed_at_ties = Window.orderBy(F.col("closed_at").desc())
w_agent_id_csat_by_csat = Window.partitionBy("agent_id", "csat").orderBy("csat", "id")
w_all_by_closed_at_cume_dist_t = Window.orderBy("closed_at_cume_dist")
w_channel_next_csat = Window.partitionBy("channel", "next_csat")
w_all_by_closed_at_cume_dist_p = Window.orderBy("closed_at_cume_dist", "id").rowsBetween(-3, -1)

valid = (
    df_tickets
    .filter(F.hour(F.col("opened_at")).between(7, 9))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

enriched = (
    valid.alias("s")
    .join(df_support_agents.alias("su"), (F.col("su.id") == F.col("s.agent_id")) & (F.col("su.hired_on").between(F.lit("2024-04-14").cast("date"), F.lit("2025-11-09").cast("date"))), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.priority",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

ranked = (
    enriched
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_channel)).alias("distinct_agent_id_in_channel"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel), 2).alias("requester_id_vs_channel_avg"),
        F.round(F.percent_rank().over(w_all_by_closed_at), 3).alias("closed_at_percent_rank"),
        F.round(F.avg("requester_id").over(w_all_by_closed_at_prior_3), 2).alias("requester_id_avg_3_prior"),
        F.rank().over(w_agent_id_by_agent_hired_on_t).alias("agent_hired_on_rank"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.round(F.cume_dist().over(w_all_by_closed_at_ties), 3).alias("closed_at_cume_dist"),
        F.min("requester_id").over(w_channel).alias("channel_min_requester_id"),
        F.max("csat").over(w_csat).alias("csat_max_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel), 2).alias("csat_vs_channel_avg"),
        F.lead("csat").over(w_agent_id_csat_by_csat).alias("next_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_csat), F.lit(0)), 4).alias("requester_id_share_of_csat"),
    )
)

sequenced = (
    ranked
    .select(
        "*",
        F.dense_rank().over(w_all_by_closed_at_cume_dist_t).alias("closed_at_cume_dist_dense_rank"),
        F.round(F.col("closed_at_percent_rank") / F.nullif(F.sum("closed_at_percent_rank").over(w_channel_next_csat), F.lit(0)), 4).alias("closed_at_percent_rank_share_of_channel_next_csa"),
        F.round(F.avg("next_csat").over(w_all_by_closed_at_cume_dist_p), 2).alias("next_csat_avg_3_prior"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_channel_next_csat), F.lit(0)), 4).alias("csat_share_of_channel_next_csat"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "tags",
        "agent_hired_on",
        "distinct_agent_id_in_channel",
        "closed_at_percent_rank",
        "requester_id_avg_3_prior",
        "agent_hired_on_rank",
        "rows_in_csat",
        "closed_at_cume_dist",
        "csat_vs_channel_avg",
        "next_csat",
        "requester_id_share_of_csat",
        "closed_at_cume_dist_dense_rank",
        "closed_at_percent_rank_share_of_channel_next_csa",
        "next_csat_avg_3_prior",
        "csat_share_of_channel_next_csat",
    )
)
