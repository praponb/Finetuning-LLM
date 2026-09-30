from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_or_unknown = Window.partitionBy("channel_or_unknown")
w_agent_team_by_agent_hired_on = Window.partitionBy("agent_team").orderBy("agent_hired_on", "id")
w_channel_or_unknown_by_csat = Window.partitionBy("channel_or_unknown").orderBy("csat", "id")
w_csat_channel_or_unknown_by_a = Window.partitionBy("csat", "channel_or_unknown").orderBy("agent_hired_on", "id")
w_channel_or_unknown_closed_at = Window.partitionBy("channel_or_unknown", F.year("closed_at_plus_2h"))
w_all_by_requester_id_ties = Window.orderBy(F.col("requester_id").desc())
w_agent_team_by_agent_hired_on_2 = Window.partitionBy("agent_team").orderBy("agent_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_requester_id_prior_1 = Window.orderBy(F.col("requester_id").desc(), "id").rowsBetween(-1, -1)
w_all_by_requester_id_ties_2 = Window.orderBy("requester_id")
w_csat_channel_or_unknown_by_a_2 = Window.partitionBy("csat", "channel_or_unknown").orderBy("agent_hired_on", "id").rowsBetween(-1, Window.currentRow)
w_prev_agent_hired_on_year_seq = Window.partitionBy(F.year("prev_agent_hired_on"), "seq_no").orderBy("distinct_agent_id_in_channel_or_unknown_closed_a", "id")
w_agent_id_closed_at_plus_2h_y = Window.partitionBy("agent_id", F.year("closed_at_plus_2h")).orderBy(F.col("requester_id_dense_rank").desc())
w_agent_id_by_agent_hired_on = Window.partitionBy("agent_id").orderBy("agent_hired_on", "id")

recent_tickets = (
    df_tickets
    .filter(F.col("channel") == "chat")
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        (F.col("closed_at") + F.expr("INTERVAL 2 HOURS")).alias("closed_at_plus_2h"),
        F.coalesce(F.col("channel"), F.lit("UNKNOWN")).alias("channel_or_unknown"),
    )
)

with_parent_agent = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.*",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

windowed = (
    with_parent_agent
    .select(
        "*",
        F.size(F.collect_set("agent_id").over(w_channel_or_unknown)).alias("distinct_agent_id_in_channel_or_unknown"),
        F.round(F.cume_dist().over(w_agent_team_by_agent_hired_on), 3).alias("agent_hired_on_cume_dist"),
        F.row_number().over(w_channel_or_unknown_by_csat).alias("seq_no"),
        F.lag("agent_hired_on").over(w_csat_channel_or_unknown_by_a).alias("prev_agent_hired_on"),
        F.size(F.collect_set("agent_id").over(w_channel_or_unknown_closed_at)).alias("distinct_agent_id_in_channel_or_unknown_closed_a"),
        F.round(F.percent_rank().over(w_all_by_requester_id_ties), 3).alias("requester_id_percent_rank"),
        F.last("agent_hired_on", ignorenulls=True).over(w_agent_team_by_agent_hired_on_2).alias("last_known_agent_hired_on"),
        F.sum("requester_id").over(w_channel_or_unknown).alias("channel_or_unknown_sum_requester_id"),
        F.round(F.avg("requester_id").over(w_all_by_requester_id_prior_1), 2).alias("requester_id_avg_1_prior"),
        F.round(F.col("csat") - F.avg("csat").over(w_channel_or_unknown_closed_at), 2).alias("csat_vs_channel_or_unknown_closed_at_plus_2h_yea"),
        F.dense_rank().over(w_all_by_requester_id_ties_2).alias("requester_id_dense_rank"),
        F.first("closed_at_plus_2h", ignorenulls=True).over(w_agent_team_by_agent_hired_on_2).alias("first_known_closed_at_plus_2h"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_channel_or_unknown), 2).alias("requester_id_vs_channel_or_unknown_avg"),
        F.round(F.avg("csat").over(w_csat_channel_or_unknown_by_a_2), 2).alias("csat_avg_2"),
    )
)

labeled = (
    windowed
    .filter(F.col("closed_at_plus_2h") < F.lit("2025-02-05 09:00:00").cast("timestamp"))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "csat",
        "tags",
        "closed_at_plus_2h",
        "channel_or_unknown",
        "agent_hired_on",
        "agent_tier",
        "agent_team",
        "seq_no",
        "prev_agent_hired_on",
        "distinct_agent_id_in_channel_or_unknown_closed_a",
        "last_known_agent_hired_on",
        "channel_or_unknown_sum_requester_id",
        "requester_id_avg_1_prior",
        "csat_vs_channel_or_unknown_closed_at_plus_2h_yea",
        "requester_id_dense_rank",
        "requester_id_vs_channel_or_unknown_avg",
        "csat_avg_2",
        F.coalesce(F.col("csat"), F.lit(0)).alias("csat_filled"),
        (F.col("csat_avg_2") + F.col("requester_id_avg_1_prior")).alias("csat_avg_2_plus_requester_id_avg_1_prior"),
    )
)

metrics = (
    labeled
    .select(
        "*",
        F.row_number().over(w_prev_agent_hired_on_year_seq).alias("distinct_agent_id_in_channel_or_unknown_closed_a_2"),
        F.round(F.cume_dist().over(w_agent_id_closed_at_plus_2h_y), 3).alias("requester_id_dense_rank_cume_dist"),
        F.ntile(4).over(w_agent_id_by_agent_hired_on).alias("agent_hired_on_quartile"),
    )
)

result = (
    metrics
    .select(
        "id",
        "requester_id",
        "agent_id",
        "tags",
        "closed_at_plus_2h",
        "agent_team",
        "seq_no",
        "distinct_agent_id_in_channel_or_unknown_closed_a",
        "last_known_agent_hired_on",
        "channel_or_unknown_sum_requester_id",
        "requester_id_avg_1_prior",
        "requester_id_dense_rank",
        "requester_id_vs_channel_or_unknown_avg",
        "csat_avg_2",
        "csat_filled",
        "csat_avg_2_plus_requester_id_avg_1_prior",
        "distinct_agent_id_in_channel_or_unknown_closed_a_2",
        "requester_id_dense_rank_cume_dist",
        "agent_hired_on_quartile",
        (F.col("csat") - F.col("requester_id_avg_1_prior")).alias("csat_minus_requester_id_avg_1_prior"),
    )
    .orderBy(F.col("channel_or_unknown_sum_requester_id").desc())
)
