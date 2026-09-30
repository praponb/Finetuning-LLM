from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_agent_tier_agent_team_by_clo = Window.partitionBy("agent_tier", "agent_team").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_team_by_closed_at = Window.partitionBy("agent_team").orderBy("closed_at", "id")
w_agent_team_by_closed_at_runn = Window.partitionBy("agent_team").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_agent_tier_by_closed_at = Window.partitionBy("agent_tier").orderBy("closed_at", "id")
w_agent_tier_by_closed_at_full = Window.partitionBy("agent_tier").orderBy("closed_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_closed_at_date = Window.orderBy(F.col("closed_at_date").desc_nulls_first(), "id")
w_agent_tier_agent_team_by_clo_2 = Window.partitionBy("agent_tier", "agent_team").orderBy("closed_at")
w_prev_closed_at_date_month_cl = Window.partitionBy(F.date_trunc("month", "prev_closed_at_date"), "closed_at_rank")
w_closed_at_dense_rank_3 = Window.partitionBy("closed_at_dense_rank_3")
w_all_by_last_closed_at_date = Window.orderBy("last_closed_at_date", "id")
w_closed_at_year_by_closed_at = Window.partitionBy(F.year("closed_at")).orderBy("closed_at_dense_rank_3", "id").rowsBetween(-6, -1)
w_last_known_closed_at_date_mo = Window.partitionBy(F.month("last_known_closed_at_date")).orderBy("closed_at_dense_rank")

recent_tickets = (
    df_tickets
    .filter((F.col("csat") <= 3) & (~F.col("channel").isin("chat", "phone")))
    .select(
        "id",
        "agent_id",
        "closed_at",
        "priority",
        "tags",
        F.to_date(F.col("closed_at")).alias("closed_at_date"),
    )
)

combined_details = (
    recent_tickets.alias("s")
    .join(df_support_agents.alias("su"), F.col("su.id") == F.col("s.agent_id"), "left")
    .select(
        "s.id",
        "s.agent_id",
        "s.closed_at",
        "s.tags",
        "s.closed_at_date",
        F.col("su.tier").alias("agent_tier"),
        F.col("su.team").alias("agent_team"),
    )
)

metrics = (
    combined_details
    .select(
        "*",
        F.first("closed_at", ignorenulls=True).over(w_agent_tier_agent_team_by_clo).alias("first_known_closed_at"),
        F.ntile(4).over(w_agent_team_by_closed_at).alias("closed_at_quartile"),
        F.last("closed_at_date", ignorenulls=True).over(w_agent_team_by_closed_at_runn).alias("last_known_closed_at_date"),
        F.lag("closed_at_date").over(w_agent_tier_by_closed_at).alias("prev_closed_at_date"),
        F.last("closed_at_date").over(w_agent_tier_by_closed_at_full).alias("last_closed_at_date"),
        F.round((F.unix_timestamp("closed_at") - F.unix_timestamp(F.lag("closed_at").over(w_all_by_closed_at_date))) / 60, 1).alias("minutes_since_prev_closed_at"),
        F.first("closed_at_date", ignorenulls=True).over(w_agent_tier_agent_team_by_clo).alias("first_known_closed_at_date"),
        F.dense_rank().over(w_agent_tier_by_closed_at).alias("closed_at_dense_rank"),
        F.dense_rank().over(w_agent_team_by_closed_at).alias("closed_at_dense_rank_2"),
        F.first("closed_at", ignorenulls=True).over(w_agent_team_by_closed_at_runn).alias("first_known_closed_at_2"),
        F.dense_rank().over(w_agent_tier_agent_team_by_clo_2).alias("closed_at_dense_rank_3"),
        F.rank().over(w_agent_tier_by_closed_at).alias("closed_at_rank"),
    )
)

with_trends = (
    metrics
    .select(
        "*",
        F.round(F.col("closed_at_dense_rank") - F.avg("closed_at_dense_rank").over(w_prev_closed_at_date_month_cl), 2).alias("closed_at_dense_rank_vs_prev_closed_at_date_mont"),
        F.size(F.collect_set("agent_team").over(w_closed_at_dense_rank_3)).alias("distinct_agent_team_in_closed_at_dense_rank_3"),
        F.size(F.collect_set("agent_team").over(w_prev_closed_at_date_month_cl)).alias("distinct_agent_team_in_prev_closed_at_date_month"),
        (F.col("minutes_since_prev_closed_at") - F.lag("minutes_since_prev_closed_at").over(w_all_by_last_closed_at_date)).alias("minutes_since_prev_closed_at_change"),
        F.max("closed_at_dense_rank").over(w_closed_at_year_by_closed_at).alias("closed_at_dense_rank_max_6_prior"),
        F.round(F.percent_rank().over(w_last_known_closed_at_date_mo), 3).alias("closed_at_dense_rank_percent_rank"),
    )
)

grouped = (
    with_trends
    .filter(F.col("closed_at_dense_rank_2") >= 2)
    .groupBy(
        F.date_format("first_known_closed_at_2", "yyyy-MM").alias("first_known_closed_at_2_ym"),
        "closed_at_dense_rank",
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("closed_at_dense_rank_vs_prev_closed_at_date_mont"), 2).alias("stddev_closed_at_dense_rank_vs_prev_closed_at_da"),
        F.max("first_known_closed_at").alias("last_first_known_closed_at"),
        F.countDistinct("agent_team").alias("distinct_agent_teams"),
        F.countDistinct("agent_tier").alias("distinct_agent_tiers"),
    )
)

result = (
    grouped
    .filter(F.col("stddev_closed_at_dense_rank_vs_prev_closed_at_da") >= 3.5)
    .select(
        "first_known_closed_at_2_ym",
        "closed_at_dense_rank",
        "cnt",
        "stddev_closed_at_dense_rank_vs_prev_closed_at_da",
        "last_first_known_closed_at",
        "distinct_agent_teams",
        "distinct_agent_tiers",
        (F.floor(F.col("closed_at_dense_rank") / 2) * 2).alias("closed_at_dense_rank_bucket"),
    )
    .orderBy(
        F.col("distinct_agent_teams").desc(),
        F.col("first_known_closed_at_2_ym").desc(),
    )
)
