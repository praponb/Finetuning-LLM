from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_hired_on = Window.orderBy("hired_on", "id")
w_hired_on_month_by_hired_on_t = Window.partitionBy(F.month("hired_on")).orderBy(F.col("hired_on").desc())
w_hired_on_month_by_hired_on = Window.partitionBy(F.month("hired_on")).orderBy(F.col("hired_on").desc(), "id")
w_tier_by_hired_on_ties = Window.partitionBy("tier").orderBy(F.col("hired_on").desc())
w_tier_by_hired_on_running = Window.partitionBy("tier").orderBy(F.col("hired_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_year_by_hired_on_fu = Window.partitionBy(F.year("hired_on")).orderBy("hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_hired_on_year_by_hired_on_ti = Window.partitionBy(F.year("hired_on")).orderBy("hired_on")
w_hired_on_seq_next3_hired_on = Window.partitionBy("hired_on_seq", F.year("next3_hired_on"))
w_hired_on_rank_by_hired_on = Window.partitionBy("hired_on_rank").orderBy(F.col("hired_on").desc(), "id")
w_hired_on_rank_team_by_first = Window.partitionBy("hired_on_rank", "team").orderBy("first_known_hired_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hired_on_rank_2_hired_on_den = Window.partitionBy("hired_on_rank_2", "hired_on_dense_rank")

scoped_agents = (
    df_support_agents
    .filter(F.length(F.trim(F.col("agent_name"))) >= 5)
    .select(
        "id",
        "agent_name",
        "team",
        "tier",
        "hired_on",
        F.lower(F.regexp_replace(F.trim(F.col("tier")), " ", "_")).alias("tier_slug"),
        F.lower(F.regexp_replace(F.trim(F.col("team")), " ", "_")).alias("team_slug"),
    )
)

ranked = (
    scoped_agents
    .select(
        "*",
        F.ntile(4).over(w_all_by_hired_on).alias("hired_on_quartile"),
        F.round(F.cume_dist().over(w_all_by_hired_on), 3).alias("hired_on_cume_dist"),
        F.rank().over(w_hired_on_month_by_hired_on_t).alias("hired_on_rank"),
        F.lead("hired_on", 3).over(w_hired_on_month_by_hired_on).alias("next3_hired_on"),
        F.row_number().over(w_hired_on_month_by_hired_on).alias("hired_on_seq"),
        F.rank().over(w_tier_by_hired_on_ties).alias("hired_on_rank_2"),
        F.first("hired_on", ignorenulls=True).over(w_tier_by_hired_on_running).alias("first_known_hired_on"),
        F.round(F.cume_dist().over(w_hired_on_month_by_hired_on_t), 3).alias("hired_on_cume_dist_2"),
        F.nth_value("hired_on", 2).over(w_hired_on_year_by_hired_on_fu).alias("second_hired_on"),
        F.dense_rank().over(w_hired_on_year_by_hired_on_ti).alias("hired_on_dense_rank"),
    )
)

top_per_group = (
    ranked
    .filter((F.col("hired_on_seq") <= 2) & (F.col("next3_hired_on").between(F.lit("2024-01-30").cast("date"), F.lit("2025-11-09").cast("date"))))
)

with_trends = (
    top_per_group
    .select(
        "*",
        F.min("hired_on_cume_dist").over(w_hired_on_seq_next3_hired_on).alias("hired_on_seq_next3_hired_on_year_min_hired_on_cu"),
        F.round(F.percent_rank().over(w_all_by_hired_on), 3).alias("hired_on_percent_rank"),
        F.size(F.collect_set("team").over(w_hired_on_seq_next3_hired_on)).alias("distinct_team_in_hired_on_seq_next3_hired_on_yea"),
        F.datediff(F.col("first_known_hired_on"), F.lag("first_known_hired_on").over(w_hired_on_rank_by_hired_on)).alias("days_since_prev_first_known_hired_on"),
        F.round(F.col("hired_on_quartile") / F.nullif(F.sum("hired_on_quartile").over(w_hired_on_seq_next3_hired_on), F.lit(0)), 4).alias("hired_on_quartile_share_of_hired_on_seq_next3_hi"),
        F.max("hired_on_rank").over(w_hired_on_rank_team_by_first).alias("max_hired_on_rank_so_far"),
        F.round(F.col("hired_on_dense_rank") / F.nullif(F.sum("hired_on_dense_rank").over(w_hired_on_rank_2_hired_on_den), F.lit(0)), 4).alias("hired_on_dense_rank_share_of_hired_on_rank_2_hir"),
    )
)

stats = (
    with_trends
    .groupBy(
        F.date_trunc("month", "first_known_hired_on").alias("first_known_hired_on_month"),
        "distinct_team_in_hired_on_seq_next3_hired_on_yea",
        "hired_on_seq",
    )
    .agg(
        F.sum("days_since_prev_first_known_hired_on").alias("total_days_since_prev_first_known_hired_on"),
        F.round(F.avg("hired_on_rank"), 2).alias("avg_hired_on_rank"),
        F.sum("hired_on_rank_2").alias("sum_hired_on_rank_2_check"),
    )
    .filter(F.col("sum_hired_on_rank_2_check") > 2)
    .drop("sum_hired_on_rank_2_check")
)

result = (
    stats
    .select(
        "first_known_hired_on_month",
        "distinct_team_in_hired_on_seq_next3_hired_on_yea",
        "hired_on_seq",
        "total_days_since_prev_first_known_hired_on",
    )
)
