from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_level = Window.partitionBy("level")
w_level_team_by_level_full = Window.partitionBy("level", "team").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_team_level_by_level = Window.partitionBy("team", "level").orderBy("level", "id")
w_team_level_by_level_ties = Window.partitionBy("team", "level").orderBy(F.col("level").desc())
w_team_level_by_level_running = Window.partitionBy("team", "level").orderBy("level", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_team = Window.partitionBy("team")
w_last_known_level_prev2_level = Window.partitionBy("last_known_level", "prev2_level")
w_team_by_team_max_level_ties = Window.partitionBy("team").orderBy(F.col("team_max_level").desc_nulls_first())
w_all_by_prev2_level_moving_1 = Window.orderBy(F.col("prev2_level").desc(), "id").rowsBetween(-1, Window.currentRow)
w_level_by_last_known_level = Window.partitionBy("level").orderBy(F.col("last_known_level").desc(), "id")
w_all_by_team_max_level_full = Window.orderBy(F.col("team_max_level").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned = (
    df_adjusters
    .select(
        "id",
        "adjuster_name",
        "team",
        "level",
    )
)

windowed = (
    cleaned
    .select(
        "*",
        F.round(F.col("level") / F.nullif(F.sum("level").over(w_level), F.lit(0)), 4).alias("level_share_of_level"),
        F.nth_value("level", 2).over(w_level_team_by_level_full).alias("second_level"),
        F.lead("level", 2).over(w_team_level_by_level).alias("next2_level"),
        F.rank().over(w_team_level_by_level_ties).alias("level_rank"),
        F.last("level", ignorenulls=True).over(w_team_level_by_level_running).alias("last_known_level"),
        F.max("level").over(w_team).alias("team_max_level"),
        F.lag("level", 2, 0).over(w_team_level_by_level).alias("prev2_level"),
        F.ntile(10).over(w_team_level_by_level).alias("level_decile"),
    )
)

annotated = (
    windowed
    .filter(F.col("next2_level").between(1, 3))
    .select(
        "*",
        F.abs(F.col("level_rank") - 4).alias("level_rank_distance"),
        F.least(F.col("level_share_of_level"), F.col("level_rank")).alias("least_level_share_of_level_level_rank"),
    )
)

windowed_2 = (
    annotated
    .select(
        "id",
        "level",
        "level_share_of_level",
        "second_level",
        "team_max_level",
        "prev2_level",
        "level_decile",
        "level_rank_distance",
        "least_level_share_of_level_level_rank",
        F.sum("next2_level").over(w_last_known_level_prev2_level).alias("last_known_level_prev2_level_sum_next2_level"),
        F.round(F.cume_dist().over(w_team_by_team_max_level_ties), 3).alias("team_max_level_cume_dist"),
        F.max("prev2_level").over(w_all_by_prev2_level_moving_1).alias("prev2_level_max_2"),
        F.ntile(3).over(w_level_by_last_known_level).alias("last_known_level_tercile"),
        F.nth_value("next2_level", 2).over(w_all_by_team_max_level_full).alias("second_next2_level"),
        F.first("level_rank").over(w_all_by_team_max_level_full).alias("first_level_rank"),
    )
)

rollup_base = (
    windowed_2
    .groupBy("level_decile")
    .agg(
        F.round(F.stddev("second_next2_level"), 2).alias("stddev_second_next2_level"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("n_rows"),
        F.percentile_approx("prev2_level", 0.5).alias("median_prev2_level"),
        F.round(F.sum("last_known_level_tercile") / F.nullif(F.sum("last_known_level_prev2_level_sum_next2_level"), F.lit(0)), 3).alias("last_known_level_tercile_to_last_known_level_pre"),
    )
)

result = (
    rollup_base
    .filter(F.col("median_prev2_level") >= 2)
    .select(
        "level_decile",
        "stddev_second_next2_level",
        "distinct_ids",
        F.least(F.col("median_prev2_level"), F.col("level_decile")).alias("least_median_prev2_level_level_decile"),
        F.greatest(F.col("distinct_ids"), F.col("n_rows")).alias("greatest_distinct_ids_n_rows"),
    )
)
