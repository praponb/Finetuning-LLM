from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_channel_agent_team_by_reques = Window.partitionBy("channel", "agent_team").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_csat_channel = Window.partitionBy("csat", "channel")
w_agent_team_channel_by_csat_f = Window.partitionBy("agent_team", "channel").orderBy("csat", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_channel_agent_team_by_reques_2 = Window.partitionBy("channel", "agent_team").orderBy(F.col("requester_id").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_closed_at_month = Window.partitionBy(F.date_trunc("month", "closed_at"))
w_agent_team_channel_by_csat_p = Window.partitionBy("agent_team", "channel").orderBy("csat", "id").rowsBetween(-3, -1)
w_agent_team_channel_by_csat = Window.partitionBy("agent_team", "channel").orderBy("csat", "id")
w_all_by_closed_at = Window.orderBy(F.col("closed_at").desc(), "id")
w_all_by_first_closed_at = Window.orderBy("first_closed_at", "id")
w_csat_agent_team = Window.partitionBy("csat", "agent_team")
w_all_by_first_closed_at_full = Window.orderBy("first_closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_first_closed_at_movin = Window.orderBy("first_closed_at", "id").rowsBetween(-2, Window.currentRow)

prepared = (
    df_tickets
    .filter((F.col("agent_id") <= 9) | (F.col("priority").isNotNull()))
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

with_details_agent = (
    prepared.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "inner")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.closed_at",
        "s.channel",
        "s.csat",
        F.col("su.team").alias("agent_team"),
        "su.agent_name",
    )
)

sequenced = (
    with_details_agent
    .select(
        "*",
        F.max("csat").over(w_channel_agent_team_by_reques).alias("max_csat_so_far"),
        F.min("csat").over(w_csat_channel).alias("csat_channel_min_csat"),
        F.first("closed_at").over(w_agent_team_channel_by_csat_f).alias("first_closed_at"),
        F.round(F.col("csat") / F.nullif(F.sum("csat").over(w_csat_channel), F.lit(0)), 4).alias("csat_share_of_csat_channel"),
        F.round(F.avg("csat").over(w_channel_agent_team_by_reques_2), 2).alias("csat_avg_4"),
        F.sum("requester_id").over(w_closed_at_month).alias("closed_at_month_sum_requester_id"),
        F.round(F.avg("requester_id").over(w_agent_team_channel_by_csat_p), 2).alias("requester_id_avg_3_prior"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_month), 2).alias("requester_id_vs_closed_at_month_avg"),
        F.lag("requester_id", 2).over(w_agent_team_channel_by_csat).alias("prev2_requester_id"),
        (F.col("csat") - F.lag("csat").over(w_all_by_closed_at)).alias("csat_change"),
    )
)

derived = sequenced.withColumn("channel_clean", F.lower(F.trim(F.col("channel"))))

with_trends = (
    derived
    .select(
        "*",
        F.lead("csat_change").over(w_all_by_first_closed_at).alias("next_csat_change"),
        F.count(F.lit(1)).over(w_csat_agent_team).alias("rows_in_csat_agent_team"),
        F.first("prev2_requester_id").over(w_all_by_first_closed_at_full).alias("first_prev2_requester_id"),
        F.max("csat_avg_4").over(w_all_by_first_closed_at_movin).alias("csat_avg_4_max_3"),
    )
)

per_group = (
    with_trends
    .filter(F.col("next_csat_change") > -4)
    .groupBy("agent_team")
    .agg(
        F.max("first_closed_at").alias("last_first_closed_at"),
        F.sum("next_csat_change").alias("total_next_csat_change"),
        F.round(F.sum("csat_avg_4_max_3") / F.nullif(F.sum("closed_at_month_sum_requester_id"), F.lit(0)), 3).alias("csat_avg_4_max_3_to_closed_at_month_sum_requeste"),
        F.sum("requester_id_avg_3_prior").alias("total_requester_id_avg_3_prior"),
        F.round(F.stddev("csat_avg_4"), 2).alias("stddev_csat_avg_4"),
        F.round(F.avg("max_csat_so_far"), 2).alias("avg_max_csat_so_far"),
    )
)

result = (
    per_group
    .select(
        "agent_team",
        "last_first_closed_at",
        "total_next_csat_change",
        "csat_avg_4_max_3_to_closed_at_month_sum_requeste",
        "total_requester_id_avg_3_prior",
        "stddev_csat_avg_4",
        "avg_max_csat_so_far",
        F.when(F.col("agent_team").isin("Onboarding", "Tech"), "group_a").when(F.col("agent_team") == "Billing", "group_b").otherwise("other").alias("agent_team_group"),
        F.round(F.col("total_next_csat_change") / F.nullif(F.col("avg_max_csat_so_far"), F.lit(0)), 3).alias("total_next_csat_change_per_avg_max_csat_so_far"),
    )
    .orderBy(F.col("agent_team").asc_nulls_last())
)
