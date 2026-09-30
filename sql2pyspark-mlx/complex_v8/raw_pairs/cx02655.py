from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_csat_agent_id_by_csat = Window.partitionBy("csat", "agent_id").orderBy(F.col("csat").desc(), "id")
w_csat = Window.partitionBy("csat")
w_channel_csat = Window.partitionBy("channel", "csat")
w_channel_by_csat_ties = Window.partitionBy("channel").orderBy("csat")
w_channel_agent_team_by_reques = Window.partitionBy("channel", "agent_team").orderBy("requester_id", "id").rowsBetween(-3, -1)
w_channel_by_csat = Window.partitionBy("channel").orderBy("csat", "id")
w_csat_agent_hired_on_month_by = Window.partitionBy("csat", F.month("agent_hired_on")).orderBy(F.col("agent_hired_on").desc())
w_agent_id_agent_team = Window.partitionBy("agent_id", "agent_team")
w_all_by_distinct_channel_in_c = Window.orderBy(F.col("distinct_channel_in_csat").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_distinct_channel_in_c_2 = Window.orderBy("distinct_channel_in_csat", "id").rowsBetween(-3, Window.currentRow)

cleaned_tickets = (
    df_tickets
    .filter((F.col("requester_id") <= 18) & (F.col("agent_id") <= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
        "tags",
    )
)

joined_agent = (
    cleaned_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.channel",
        "s.csat",
        "s.tags",
        F.col("su.team").alias("agent_team"),
        F.col("su.hired_on").alias("agent_hired_on"),
    )
)

windowed = (
    joined_agent
    .select(
        "*",
        F.datediff(F.col("agent_hired_on"), F.lag("agent_hired_on").over(w_csat_agent_id_by_csat)).alias("days_since_prev_agent_hired_on"),
        F.round(F.avg("csat").over(w_csat), 2).alias("csat_avg_csat"),
        F.min("requester_id").over(w_channel_csat).alias("channel_csat_min_requester_id"),
        F.rank().over(w_channel_by_csat_ties).alias("csat_rank"),
        F.size(F.collect_set("channel").over(w_csat)).alias("distinct_channel_in_csat"),
        F.count(F.lit(1)).over(w_csat).alias("rows_in_csat"),
        F.sum("requester_id").over(w_csat).alias("csat_sum_requester_id"),
        F.round(F.avg("csat").over(w_channel_agent_team_by_reques), 2).alias("csat_avg_3_prior"),
        F.lead("csat").over(w_channel_by_csat).alias("next_csat"),
        F.round(F.col("requester_id") / F.nullif(F.sum("requester_id").over(w_channel_csat), F.lit(0)), 4).alias("requester_id_share_of_channel_csat"),
        F.size(F.collect_set("agent_id").over(w_channel_csat)).alias("distinct_agent_id_in_channel_csat"),
        F.round(F.percent_rank().over(w_csat_agent_hired_on_month_by), 3).alias("agent_hired_on_percent_rank"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.round(F.col("csat_rank") - F.avg("csat_rank").over(w_agent_id_agent_team), 2).alias("csat_rank_vs_agent_id_agent_team_avg"),
        F.round(F.avg("csat_sum_requester_id").over(w_all_by_distinct_channel_in_c), 2).alias("csat_sum_requester_id_avg_3"),
        F.round(F.avg("agent_hired_on_percent_rank").over(w_agent_id_agent_team), 2).alias("agent_id_agent_team_avg_agent_hired_on_percent_r"),
        F.max("next_csat").over(w_all_by_distinct_channel_in_c_2).alias("next_csat_max_4"),
    )
)

stats = (
    with_trends
    .groupBy(
        "next_csat",
        F.date_format("agent_hired_on", "yyyy-MM").alias("agent_hired_on_ym"),
        "channel",
    )
    .agg(
        F.array_sort(F.collect_set("channel")).alias("channel_list"),
        F.count("*").alias("row_count"),
    )
)

result = (
    stats
    .filter(F.size(F.col("channel_list")) >= 1)
    .select(
        "next_csat",
        "agent_hired_on_ym",
        "channel",
        "channel_list",
    )
    .orderBy("channel", F.col("next_csat").desc())
)
