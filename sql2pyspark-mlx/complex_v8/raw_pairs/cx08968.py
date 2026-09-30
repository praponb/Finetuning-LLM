from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc(), "id")
w_tier_team_by_hired_on_full = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tier_team_by_hired_on = Window.partitionBy("tier", "team").orderBy(F.col("hired_on").desc(), "id")
w_team_hired_on_rank = Window.partitionBy("team", "hired_on_rank")
w_prev_hired_on_month_by_prev = Window.partitionBy(F.date_trunc("month", "prev_hired_on")).orderBy(F.col("prev_hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_rank_second_hired_o = Window.partitionBy("hired_on_rank", "second_hired_on_month").orderBy("prev_hired_on", "id")

base = (
    df_support_agents
    .filter(F.col("tier") != "T2")
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.concat_ws(" / ", F.col("agent_name"), F.col("tier")).alias("agent_name_tier_label"),
        F.initcap(F.trim(F.col("tier"))).alias("tier_clean"),
        F.upper(F.trim(F.col("tier"))).alias("tier_clean_2"),
    )
)

sequenced = (
    base
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "tier_clean",
        "tier_clean_2",
        F.rank().over(w_all_by_hired_on).alias("hired_on_rank"),
        F.lag("hired_on").over(w_all_by_hired_on).alias("prev_hired_on"),
        F.nth_value("hired_on", 2).over(w_tier_team_by_hired_on_full).alias("second_hired_on"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_tier_team_by_hired_on)).alias("days_since_prev_hired_on"),
        F.round(F.percent_rank().over(w_tier_team_by_hired_on), 3).alias("hired_on_percent_rank"),
    )
)

labeled = (
    sequenced
    .filter(F.upper(F.trim(F.col("tier_clean"))).like("T%"))
    .select(
        "*",
        F.coalesce(F.col("hired_on_percent_rank"), F.lit(0.0)).alias("hired_on_percent_rank_filled"),
        F.month(F.col("second_hired_on")).alias("second_hired_on_month"),
    )
)

with_windows = (
    labeled
    .select(
        "id",
        "agent_name",
        "tier_clean",
        "hired_on_rank",
        "prev_hired_on",
        "days_since_prev_hired_on",
        "hired_on_percent_rank_filled",
        "second_hired_on_month",
        F.max("hired_on_percent_rank_filled").over(w_team_hired_on_rank).alias("team_hired_on_rank_max_hired_on_percent_rank_fil"),
        F.count(F.lit(1)).over(w_prev_hired_on_month_by_prev).alias("running_count"),
        F.size(F.collect_set("tier").over(w_team_hired_on_rank)).alias("distinct_tier_in_team_hired_on_rank"),
        F.count(F.lit(1)).over(w_team_hired_on_rank).alias("rows_in_team_hired_on_rank"),
        F.round(F.cume_dist().over(w_hired_on_rank_second_hired_o), 3).alias("prev_hired_on_cume_dist"),
    )
)

result = (
    with_windows
    .filter(F.col("rows_in_team_hired_on_rank") > 3)
    .select(
        "id",
        "agent_name",
        "hired_on_rank",
        "days_since_prev_hired_on",
        "hired_on_percent_rank_filled",
        "second_hired_on_month",
        "team_hired_on_rank_max_hired_on_percent_rank_fil",
        "running_count",
        "distinct_tier_in_team_hired_on_rank",
        "rows_in_team_hired_on_rank",
        "prev_hired_on_cume_dist",
        F.substring(F.trim(F.col("tier_clean")), 1, 2).alias("tier_clean_prefix"),
        F.last_day(F.col("prev_hired_on")).alias("prev_hired_on_month_end"),
    )
    .orderBy(
        F.col("distinct_tier_in_team_hired_on_rank").asc(),
        "prev_hired_on_cume_dist",
    )
)
