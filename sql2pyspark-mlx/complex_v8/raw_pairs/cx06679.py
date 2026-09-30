from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_by_csat_prior_3 = Window.partitionBy("csat").orderBy("csat", "id").rowsBetween(-3, -1)
w_all_by_csat_ties = Window.orderBy(F.col("csat").desc())
w_channel_csat_by_csat_running = Window.partitionBy("channel", "csat").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel_or_unknown = Window.partitionBy("csat", "channel_or_unknown")
w_agent_id = Window.partitionBy("agent_id")
w_all_by_csat = Window.orderBy(F.col("csat").desc(), "id")
w_agent_id_by_agent_id_min_csa = Window.partitionBy("agent_id").orderBy(F.col("agent_id_min_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel = Window.partitionBy("channel")
w_agent_id_by_agent_id_min_csa_2 = Window.partitionBy("agent_id").orderBy(F.col("agent_id_min_csat").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_agent_id_by_agent_id_min_csa_3 = Window.partitionBy("agent_id").orderBy(F.col("agent_id_min_csat").desc_nulls_first(), "id")
w_agent_team_by_csat_rank_runn = Window.partitionBy("agent_team").orderBy("csat_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel = Window.partitionBy("csat", "channel")

recent_tickets = (
    df_tickets
    .filter(F.col("agent_id") >= 9)
    .select(
        "id",
        "requester_id",
        "agent_id",
        "channel",
        "csat",
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
        F.substring(F.trim(F.col("channel")), 1, 2).alias("channel_prefix"),
    )
)

joined = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.channel_or_unknown",
        "su.agent_name",
        F.col("su.team").alias("agent_team"),
    )
)

sequenced = (
    joined
    .select(
        "id",
        "agent_id",
        "channel",
        "csat",
        "channel_or_unknown",
        "agent_name",
        "agent_team",
        F.round(F.avg("csat").over(w_csat_by_csat_prior_3), 2).alias("csat_avg_3_prior"),
        F.round(F.percent_rank().over(w_all_by_csat_ties), 3).alias("csat_percent_rank"),
        F.min("csat").over(w_channel_csat_by_csat_running).alias("min_csat_so_far"),
        F.min("csat").over(w_csat_channel_or_unknown).alias("csat_channel_or_unknown_min_csat"),
        F.round(F.col("csat") - F.avg("csat").over(w_agent_id), 2).alias("csat_vs_agent_id_avg"),
        F.round(F.col("csat") - F.avg("csat").over(w_csat_channel_or_unknown), 2).alias("csat_vs_csat_channel_or_unknown_avg"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_channel_or_unknown), F.lit(0)), 4).alias("csat_share_of_csat_channel_or_unknown"),
        F.min("csat").over(w_agent_id).alias("agent_id_min_csat"),
        F.count(F.lit(1)).over(w_agent_id).alias("rows_in_agent_id"),
        F.rank().over(w_all_by_csat_ties).alias("csat_rank"),
        F.ntile(3).over(w_all_by_csat).alias("csat_tercile"),
    )
)

first_rows = sequenced.filter((F.col("csat_rank") == 1) & (F.col("channel_or_unknown") == "web"))

with_windows = (
    first_rows
    .select(
        "id",
        "agent_id",
        "channel",
        "channel_or_unknown",
        "agent_name",
        "agent_team",
        "csat_avg_3_prior",
        "min_csat_so_far",
        "csat_channel_or_unknown_min_csat",
        "csat_vs_csat_channel_or_unknown_avg",
        "rows_in_agent_id",
        "csat_rank",
        "csat_tercile",
        F.max("min_csat_so_far").over(w_agent_id_by_agent_id_min_csa).alias("max_min_csat_so_far_so_far"),
        F.round(F.col("csat_percent_rank") / F.nullif(F.sum("csat_percent_rank").over(w_channel), F.lit(0)), 4).alias("csat_percent_rank_share_of_channel"),
        F.round(F.col("csat_tercile") / F.nullif(F.sum("csat_tercile").over(w_channel), F.lit(0)), 4).alias("csat_tercile_share_of_channel"),
        F.nth_value("csat", 2).over(w_agent_id_by_agent_id_min_csa_2).alias("second_csat"),
        F.lag("csat_rank").over(w_agent_id_by_agent_id_min_csa_3).alias("prev_csat_rank"),
        F.count(F.lit(1)).over(w_agent_team_by_csat_rank_runn).alias("running_count"),
        F.round(F.col("csat_vs_agent_id_avg") - F.avg("csat_vs_agent_id_avg").over(w_channel), 2).alias("csat_vs_agent_id_avg_vs_channel_avg"),
        F.max("agent_id_min_csat").over(w_csat_channel).alias("csat_channel_max_agent_id_min_csat"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "agent_id",
        "channel",
        "channel_or_unknown",
        "agent_name",
        "csat_avg_3_prior",
        "csat_channel_or_unknown_min_csat",
        "rows_in_agent_id",
        "csat_rank",
        "csat_tercile",
        "max_min_csat_so_far_so_far",
        "csat_percent_rank_share_of_channel",
        "csat_tercile_share_of_channel",
        "second_csat",
        "running_count",
        "csat_vs_agent_id_avg_vs_channel_avg",
        "csat_channel_max_agent_id_min_csat",
        F.abs(F.col("csat_rank") - 6).alias("csat_rank_distance"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown_2"),
    )
    .orderBy(F.col("max_min_csat_so_far_so_far").desc(), F.col("channel").desc(), "id")
    .limit(20)
)
