from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_id = Window.partitionBy("agent_id")
w_csat_has_refund_by_closed_at = Window.partitionBy("csat", "has_refund").orderBy("closed_at", "id")
w_agent_tier_agent_id_by_reque = Window.partitionBy("agent_tier", "agent_id").orderBy(F.col("requester_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_id_agent_tier_by_csat = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("csat").asc_nulls_last())
w_has_refund_agent_tier = Window.partitionBy("has_refund", "agent_tier")
w_agent_team_has_refund_by_csa = Window.partitionBy("agent_team", "has_refund").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_has_refund_by_closed_at_2 = Window.partitionBy("csat", "has_refund").orderBy("closed_at")
w_agent_id_agent_tier_by_csat_2 = Window.partitionBy("agent_id", "agent_tier").orderBy(F.col("csat").asc_nulls_last(), "id")
w_csat = Window.partitionBy("csat")
w_agent_team_last_csat_by_next = Window.partitionBy("agent_team", "last_csat").orderBy("next_csat_per_requester_id", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_agent_team_by_csat = Window.partitionBy("csat", "agent_team").orderBy(F.col("csat").asc_nulls_last(), "id")
w_agent_team_last_csat_by_next_2 = Window.partitionBy("agent_team", "last_csat").orderBy("next_csat_per_requester_id", "id")
w_csat_agent_team_by_csat_ties = Window.partitionBy("csat", "agent_team").orderBy(F.col("csat").asc_nulls_last())

prepared = (
    df_tickets
    .filter((F.col("closed_at") < F.lit("2025-02-10 04:00:00").cast("timestamp")) & (F.col("agent_id") >= 7))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
        F.round(F.col("csat") / F.nullif(F.col("requester_id"), F.lit(0)), 3).alias("csat_per_requester_id"),
        F.array_contains(F.col("tags"), "refund").alias("has_refund"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.csat",
        "s.csat_per_requester_id",
        "s.has_refund",
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
        "su.agent_name",
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.max("requester_id").over(w_agent_id).alias("agent_id_max_requester_id"),
        F.row_number().over(w_csat_has_refund_by_closed_at).alias("seq_no"),
        F.sum("csat_per_requester_id").over(w_agent_tier_agent_id_by_reque).alias("running_csat_per_requester_id"),
        F.round(F.cume_dist().over(w_agent_id_agent_tier_by_csat), 3).alias("csat_cume_dist"),
        F.sum("requester_id").over(w_agent_id).alias("agent_id_sum_requester_id"),
        F.max("csat_per_requester_id").over(w_has_refund_agent_tier).alias("has_refund_agent_tier_max_csat_per_requester_id"),
        F.max("csat").over(w_agent_id).alias("agent_id_max_csat"),
        F.last("csat").over(w_agent_team_has_refund_by_csa).alias("last_csat"),
        F.round(F.percent_rank().over(w_csat_has_refund_by_closed_at_2), 3).alias("closed_at_percent_rank"),
        F.nth_value("closed_at", 2).over(w_agent_team_has_refund_by_csa).alias("second_closed_at"),
        F.lead("csat_per_requester_id").over(w_csat_has_refund_by_closed_at).alias("next_csat_per_requester_id"),
        F.ntile(10).over(w_agent_id_agent_tier_by_csat_2).alias("csat_decile"),
        F.round(F.col("csat_per_requester_id") - F.avg("csat_per_requester_id").over(w_agent_id), 2).alias("csat_per_requester_id_vs_agent_id_avg"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("seq_no") <= 3)
    .drop("seq_no")
)

sequenced = (
    top_per_group
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "csat",
        "csat_per_requester_id",
        "has_refund",
        "agent_team",
        "agent_tier",
        "agent_name",
        "agent_id_max_requester_id",
        "running_csat_per_requester_id",
        "csat_cume_dist",
        "agent_id_sum_requester_id",
        "agent_id_max_csat",
        "next_csat_per_requester_id",
        "csat_per_requester_id_vs_agent_id_avg",
        F.size(F.collect_set("agent_team").over(w_csat)).alias("distinct_agent_team_in_csat"),
        F.last("csat_per_requester_id").over(w_agent_team_last_csat_by_next).alias("last_csat_per_requester_id"),
        F.ntile(10).over(w_csat_agent_team_by_csat).alias("csat_decile_2"),
        F.lead("agent_id_sum_requester_id").over(w_agent_team_last_csat_by_next_2).alias("next_agent_id_sum_requester_id"),
        F.round(F.avg("csat_per_requester_id").over(w_csat), 2).alias("csat_avg_csat_per_requester_id"),
        F.min("closed_at_percent_rank").over(w_csat).alias("csat_min_closed_at_percent_rank"),
        F.round(F.cume_dist().over(w_csat_agent_team_by_csat_ties), 3).alias("csat_cume_dist_2"),
    )
)

result = (
    sequenced
    .filter(F.col("next_csat_per_requester_id") <= 7.5)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "closed_at",
        "has_refund",
        "agent_name",
        "agent_id_max_requester_id",
        "running_csat_per_requester_id",
        "csat_cume_dist",
        "agent_id_max_csat",
        "next_csat_per_requester_id",
        "csat_per_requester_id_vs_agent_id_avg",
        "distinct_agent_team_in_csat",
        "last_csat_per_requester_id",
        "csat_decile_2",
        "next_agent_id_sum_requester_id",
        "csat_avg_csat_per_requester_id",
        "csat_min_closed_at_percent_rank",
    )
    .orderBy(F.col("next_agent_id_sum_requester_id").desc())
)
