from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_closed_at_month = Window.partitionBy(F.date_trunc("month", "closed_at"))
w_channel_csat_by_closed_at = Window.partitionBy("channel", "csat").orderBy("closed_at", "id")
w_csat_closed_at_to_date_by_re = Window.partitionBy("csat", F.to_date("closed_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_csat_closed_at_to_date_by_re_2 = Window.partitionBy("csat", F.to_date("closed_at")).orderBy(F.col("requester_id").desc())
w_channel_agent_team_by_csat = Window.partitionBy("channel", "agent_team").orderBy(F.col("csat").asc_nulls_last(), "id")
w_csat_closed_at_to_date_by_re_3 = Window.partitionBy("csat", F.to_date("closed_at")).orderBy(F.col("requester_id").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_channel_agent_team_by_csat_p = Window.partitionBy("channel", "agent_team").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_channel_agent_team_by_csat_r = Window.partitionBy("channel", "agent_team").orderBy(F.col("csat").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_channel_in_closed_a = Window.partitionBy("distinct_channel_in_closed_at_month").orderBy("closed_at", "id")
w_distinct_channel_in_closed_a_2 = Window.partitionBy("distinct_channel_in_closed_at_month").orderBy("closed_at", "id").rowsBetween(-2, Window.currentRow)
w_distinct_channel_in_closed_a_3 = Window.partitionBy("distinct_channel_in_closed_at_month")

base = (
    df_tickets
    .filter((F.hour(F.col("opened_at")).between(10, 16)) & (F.col("csat") <= 3))
    .select(
        "id",
        "requester_id",
        "agent_id",
        "opened_at",
        "closed_at",
        "priority",
        "channel",
        "csat",
    )
)

enriched_agent = (
    base.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.requester_id",
        "s.agent_id",
        "s.opened_at",
        "s.closed_at",
        "s.channel",
        "s.csat",
        F.col("su.hired_on").alias("agent_hired_on"),
        F.col("su.team").alias("agent_team"),
        F.col("su.tier").alias("agent_tier"),
    )
)

windowed = (
    enriched_agent
    .select(
        "*",
        F.size(F.collect_set("channel").over(w_closed_at_month)).alias("distinct_channel_in_closed_at_month"),
        F.rank().over(w_channel_csat_by_closed_at).alias("closed_at_rank"),
        F.round((F.unix_timestamp("opened_at") - F.unix_timestamp(F.lag("opened_at").over(w_channel_csat_by_closed_at))) / 60, 1).alias("minutes_since_prev_opened_at"),
        F.min("requester_id").over(w_closed_at_month).alias("closed_at_month_min_requester_id"),
        F.nth_value("closed_at", 2).over(w_csat_closed_at_to_date_by_re).alias("second_closed_at"),
        F.rank().over(w_csat_closed_at_to_date_by_re_2).alias("requester_id_rank"),
        F.lead("csat", 2, 0).over(w_channel_agent_team_by_csat).alias("next2_csat"),
        F.count(F.lit(1)).over(w_closed_at_month).alias("rows_in_closed_at_month"),
        F.first("requester_id", ignorenulls=True).over(w_csat_closed_at_to_date_by_re_3).alias("first_known_requester_id"),
        F.ntile(5).over(w_channel_agent_team_by_csat).alias("csat_quintile"),
        F.round(F.col("requester_id") - F.avg("requester_id").over(w_closed_at_month), 2).alias("requester_id_vs_closed_at_month_avg"),
        F.max("requester_id").over(w_channel_agent_team_by_csat_p).alias("requester_id_max_2_prior"),
        F.first("agent_hired_on", ignorenulls=True).over(w_channel_agent_team_by_csat_r).alias("first_known_agent_hired_on"),
    )
)

leaders = (
    windowed
    .filter(F.col("requester_id_rank") <= 3)
    .drop("requester_id_rank")
)

windowed_2 = (
    leaders
    .select(
        "id",
        "opened_at",
        "csat",
        "agent_team",
        "agent_tier",
        "distinct_channel_in_closed_at_month",
        "closed_at_rank",
        "closed_at_month_min_requester_id",
        "second_closed_at",
        "next2_csat",
        "first_known_requester_id",
        "requester_id_vs_closed_at_month_avg",
        "requester_id_max_2_prior",
        F.round(F.percent_rank().over(w_distinct_channel_in_closed_a), 3).alias("closed_at_percent_rank"),
        F.sum("first_known_requester_id").over(w_distinct_channel_in_closed_a_2).alias("first_known_requester_id_sum_3"),
        F.round(F.col("first_known_requester_id") / F.nullif(F.sum("first_known_requester_id").over(w_distinct_channel_in_closed_a_3), F.lit(0)), 4).alias("first_known_requester_id_share_of_distinct_chann"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "csat",
        "agent_team",
        "agent_tier",
        "closed_at_rank",
        "closed_at_month_min_requester_id",
        "next2_csat",
        "requester_id_vs_closed_at_month_avg",
        "requester_id_max_2_prior",
        "closed_at_percent_rank",
        "first_known_requester_id_share_of_distinct_chann",
        F.lower(F.regexp_replace(F.trim(F.col("agent_team")), " ", "_")).alias("agent_team_slug"),
        F.round(F.col("first_known_requester_id") / F.nullif(F.col("closed_at_rank"), F.lit(0)), 3).alias("first_known_requester_id_per_closed_at_rank"),
    )
)
