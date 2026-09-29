from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy(F.col("hired_on").desc(), "id")
w_tier_hired_on_year_by_hired = Window.partitionBy("tier", F.year("hired_on")).orderBy("hired_on", "id")
w_tier_by_hired_on = Window.partitionBy("tier").orderBy(F.col("hired_on").asc_nulls_last(), "id")
w_tier_hired_on_year_by_hired_2 = Window.partitionBy("tier", F.year("hired_on")).orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_hired_on_year_by_hired_3 = Window.partitionBy("tier", F.year("hired_on")).orderBy("hired_on")
w_tier_by_hired_on_running = Window.partitionBy("tier").orderBy(F.col("hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tier_by_hired_on_full = Window.partitionBy("tier").orderBy(F.col("hired_on").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_hired_on_running = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on_full = Window.orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tier_hired_on_year_by_hired_4 = Window.partitionBy("tier", F.year("hired_on")).orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_hired_on_rank_team_by_hired = Window.partitionBy("hired_on_rank", "team").orderBy("hired_on_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_hired_on_cume_dist_mo = Window.orderBy("hired_on_cume_dist", "id").rowsBetween(-6, Window.currentRow)
w_hired_on_rank_by_second_hire = Window.partitionBy("hired_on_rank").orderBy(F.col("second_hired_on_2").desc(), "id").rowsBetween(-1, Window.currentRow)
w_hired_on_rank_team_by_hired_2 = Window.partitionBy("hired_on_rank", "team").orderBy("hired_on_dense_rank", "id")

recent_agents = df_support_agents.filter(F.col("hired_on").isNotNull())

windowed = (
    recent_agents
    .select(
        "*",
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_all_by_hired_on)).alias("days_since_prev_hired_on"),
        F.round(F.percent_rank().over(w_tier_hired_on_year_by_hired), 3).alias("hired_on_percent_rank"),
        F.round(F.percent_rank().over(w_all_by_hired_on), 3).alias("hired_on_percent_rank_2"),
        F.rank().over(w_tier_by_hired_on).alias("hired_on_rank"),
        F.last("hired_on", ignorenulls=True).over(w_tier_hired_on_year_by_hired_2).alias("last_known_hired_on"),
        F.dense_rank().over(w_tier_hired_on_year_by_hired_3).alias("hired_on_dense_rank"),
        F.last("hired_on", ignorenulls=True).over(w_tier_by_hired_on_running).alias("last_known_hired_on_2"),
        F.nth_value("hired_on", 2).over(w_tier_by_hired_on_full).alias("second_hired_on"),
        F.round(F.cume_dist().over(w_all_by_hired_on), 3).alias("hired_on_cume_dist"),
        F.datediff(F.col("hired_on"), F.lag("hired_on").over(w_tier_hired_on_year_by_hired)).alias("days_since_prev_hired_on_2"),
        F.rank().over(w_all_by_hired_on).alias("hired_on_rank_2"),
        F.lag("hired_on", 2).over(w_tier_by_hired_on).alias("prev2_hired_on"),
        F.last("hired_on", ignorenulls=True).over(w_all_by_hired_on_running).alias("last_known_hired_on_3"),
        F.nth_value("hired_on", 2).over(w_all_by_hired_on_full).alias("second_hired_on_2"),
        F.first("hired_on").over(w_tier_hired_on_year_by_hired_4).alias("first_hired_on"),
    )
)

with_trends = (
    windowed
    .select(
        "*",
        F.max("hired_on_rank").over(w_hired_on_rank_team_by_hired).alias("max_hired_on_rank_so_far"),
        F.max("days_since_prev_hired_on").over(w_all_by_hired_on_cume_dist_mo).alias("days_since_prev_hired_on_max_7"),
        F.max("hired_on_rank_2").over(w_hired_on_rank_by_second_hire).alias("hired_on_rank_2_max_2"),
        F.ntile(3).over(w_hired_on_rank_team_by_hired_2).alias("hired_on_dense_rank_tercile"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "agent_name",
        "tier",
        "hired_on_percent_rank",
        "hired_on_rank",
        "last_known_hired_on",
        "hired_on_dense_rank",
        "last_known_hired_on_2",
        "second_hired_on",
        "hired_on_cume_dist",
        "days_since_prev_hired_on_2",
        "hired_on_rank_2",
        "prev2_hired_on",
        "second_hired_on_2",
        "first_hired_on",
        "max_hired_on_rank_so_far",
        "hired_on_rank_2_max_2",
        "hired_on_dense_rank_tercile",
        F.date_trunc("month", F.col("last_known_hired_on_3")).alias("last_known_hired_on_3_month"),
    )
    .orderBy(
        F.col("hired_on_dense_rank").desc(),
        F.col("hired_on_rank_2_max_2").asc_nulls_last(),
    )
)
