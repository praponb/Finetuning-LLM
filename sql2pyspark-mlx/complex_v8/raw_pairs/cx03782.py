from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dept_by_credits_running = Window.partitionBy("dept").orderBy(F.col("credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_credits_level_len = Window.partitionBy("credits", "level_len")
w_credits_distance_by_credits = Window.partitionBy("credits_distance").orderBy("credits", "id").rowsBetween(-3, Window.currentRow)
w_credits_distance_by_credits_2 = Window.partitionBy("credits_distance").orderBy("credits", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_credits_distance_dept_by_cre = Window.partitionBy("credits_distance", "dept").orderBy("credits_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dept_by_credits_ties = Window.partitionBy("dept").orderBy(F.col("credits").asc_nulls_last())
w_second_credits = Window.partitionBy("second_credits")
w_first_level_len_credits_dist = Window.partitionBy("first_level_len", "credits_distance").orderBy("last_known_credits_distance", "id")
w_first_level_len_credits_dist_2 = Window.partitionBy("first_level_len", "credits_distance").orderBy("last_known_credits_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_row_count_min_last_known_cre = Window.partitionBy("row_count", "min_last_known_credits_distance")
w_all_by_n_level_in_core_intro = Window.orderBy(F.col("n_level_in_core_intro").desc())

base_courses = (
    df_courses
    .filter((F.col("credits") < 3) & (~F.col("level").isin("advanced", "core")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.length(F.trim(F.col("level"))).alias("level_len"),
        F.abs(F.col("credits") - 2).alias("credits_distance"),
    )
)

with_trends = (
    base_courses
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        "level_len",
        "credits_distance",
        F.last("credits_distance", ignorenulls=True).over(w_dept_by_credits_running).alias("last_known_credits_distance"),
        F.min("credits").over(w_credits_level_len).alias("credits_level_len_min_credits"),
        F.max("level_len").over(w_credits_distance_by_credits).alias("level_len_max_4"),
        F.round(F.col("credits_distance") / F.nullif(F.sum("credits_distance").over(w_credits_level_len), F.lit(0)), 4).alias("credits_distance_share_of_credits_level_len"),
        F.first("level_len").over(w_credits_distance_by_credits_2).alias("first_level_len"),
        F.nth_value("credits", 2).over(w_credits_distance_dept_by_cre).alias("second_credits"),
        F.rank().over(w_dept_by_credits_ties).alias("credits_rank"),
    )
)

sequenced = (
    with_trends
    .select(
        "*",
        F.round(F.avg("first_level_len").over(w_second_credits), 2).alias("second_credits_avg_first_level_len"),
        F.lag("level_len").over(w_first_level_len_credits_dist).alias("prev_level_len"),
        F.row_number().over(w_first_level_len_credits_dist).alias("seq_no"),
        F.nth_value("level_len_max_4", 2).over(w_first_level_len_credits_dist_2).alias("second_level_len_max_4"),
    )
)

per_group = (
    sequenced
    .filter(F.col("last_known_credits_distance") > 1)
    .groupBy("credits_distance", "second_credits")
    .agg(
        F.count(F.when(F.col("level").isin("core", "intro"), 1)).alias("n_level_in_core_intro"),
        F.percentile_approx("second_level_len_max_4", 0.5).alias("median_second_level_len_max_4"),
        F.count("*").alias("row_count"),
        F.sum("level_len").alias("total_level_len"),
        F.min("last_known_credits_distance").alias("min_last_known_credits_distance"),
    )
)

metrics = (
    per_group
    .select(
        "*",
        F.round(F.col("credits_distance") / F.nullif(F.sum("credits_distance").over(w_row_count_min_last_known_cre), F.lit(0)), 4).alias("credits_distance_share_of_row_count_min_last_kno"),
        F.rank().over(w_all_by_n_level_in_core_intro).alias("n_level_in_core_intro_rank"),
    )
)

result = (
    metrics
    .select(
        "credits_distance",
        "n_level_in_core_intro",
        "total_level_len",
        "min_last_known_credits_distance",
        "credits_distance_share_of_row_count_min_last_kno",
        "n_level_in_core_intro_rank",
        F.greatest(F.col("row_count"), F.col("n_level_in_core_intro")).alias("greatest_row_count_n_level_in_core_intro"),
    )
    .orderBy(
        F.col("total_level_len").desc(),
        F.col("greatest_row_count_n_level_in_core_intro").desc_nulls_first(),
    )
)
