from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_by_score_distance_ties = Window.partitionBy("term").orderBy(F.col("score_distance").asc_nulls_last())
w_course_level_by_course_credi = Window.partitionBy("course_level").orderBy("course_credits")
w_course_level_by_course_credi_2 = Window.partitionBy("course_level").orderBy("course_credits", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_term = Window.partitionBy("term")
w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score_distance_runni = Window.partitionBy("term").orderBy(F.col("score_distance").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_level_by_course_credi_3 = Window.partitionBy("course_level").orderBy("course_credits", "id")
w_all_by_score_ties = Window.orderBy(F.col("score").desc())
w_all_by_enrolled_at = Window.orderBy("enrolled_at", "id")
w_all_by_enrolled_at_running = Window.orderBy("enrolled_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_by_score_distance = Window.partitionBy("term").orderBy(F.col("score_distance").asc_nulls_last(), "id")
w_next_course_credits = Window.partitionBy("next_course_credits")
w_all_by_score_distance_percen = Window.orderBy("score_distance_percent_rank", "id")
w_all_by_score_distance_cume_d = Window.orderBy("score_distance_cume_dist", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_distance_cume_d_2 = Window.orderBy("score_distance_cume_dist")

recent_enrollments = (
    df_enrollments
    .filter((F.col("score").between(26.2, 74.9)) & (~F.col("term").isin("2024T1", "2024T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.abs(F.col("score") - 43.5).alias("score_distance"),
    )
)

combined_details = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.level") == "core"), "inner")
    .filter(F.col("c.dept") != "Bio")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.score_distance",
        F.col("c.credits").alias("course_credits"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

sequenced = (
    combined_details
    .select(
        "*",
        F.round(F.percent_rank().over(w_term_by_score_distance_ties), 3).alias("score_distance_percent_rank"),
        F.rank().over(w_course_level_by_course_credi).alias("course_credits_rank"),
        F.first("course_credits", ignorenulls=True).over(w_course_level_by_course_credi_2).alias("first_known_course_credits"),
        F.max("score").over(w_course_id).alias("course_id_max_score"),
        F.round(F.col("course_credits") - F.avg("course_credits").over(w_term), 2).alias("course_credits_vs_term_avg"),
        F.count(F.lit(1)).over(w_all_by_score_running).alias("running_count"),
        F.sum("course_credits").over(w_term_by_score_distance_runni).alias("running_course_credits"),
        F.round((F.unix_timestamp("enrolled_at") - F.unix_timestamp(F.lag("enrolled_at").over(w_course_level_by_course_credi_3))) / 60, 1).alias("minutes_since_prev_enrolled_at"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.min("score_distance").over(w_course_id).alias("course_id_min_score_distance"),
        F.lead("course_credits").over(w_all_by_enrolled_at).alias("next_course_credits"),
        F.first("course_credits", ignorenulls=True).over(w_all_by_enrolled_at_running).alias("first_known_course_credits_2"),
        F.ntile(5).over(w_all_by_enrolled_at).alias("enrolled_at_quintile"),
        F.round(F.cume_dist().over(w_term_by_score_distance), 3).alias("score_distance_cume_dist"),
    )
)

with_windows = (
    sequenced
    .select(
        "*",
        F.min("first_known_course_credits_2").over(w_next_course_credits).alias("next_course_credits_min_first_known_course_credi"),
        F.round(F.cume_dist().over(w_all_by_score_distance_percen), 3).alias("score_distance_percent_rank_cume_dist"),
        F.last("course_id_min_score_distance").over(w_all_by_score_distance_cume_d).alias("last_course_id_min_score_distance"),
        F.rank().over(w_all_by_score_distance_cume_d_2).alias("score_distance_cume_dist_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "term",
        "enrolled_at",
        "score_distance",
        "course_credits",
        "course_dept",
        "course_credits_rank",
        "course_credits_vs_term_avg",
        "running_count",
        "minutes_since_prev_enrolled_at",
        "score_percent_rank",
        "next_course_credits",
        "first_known_course_credits_2",
        "enrolled_at_quintile",
        "score_distance_cume_dist",
        "next_course_credits_min_first_known_course_credi",
        "score_distance_percent_rank_cume_dist",
        "last_course_id_min_score_distance",
        "score_distance_cume_dist_rank",
        (F.col("score_distance") + F.col("running_count")).alias("score_distance_plus_running_count"),
    )
    .orderBy(F.col("score_distance_cume_dist_rank").desc())
)
