from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_course_level = Window.partitionBy("student_id", "course_level")
w_course_level_course_id = Window.partitionBy("course_level", "course_id")
w_grade_status_course_level_by = Window.partitionBy("grade_status", "course_level").orderBy(F.col("course_credits").asc_nulls_last(), "id")
w_grade_status_course_level_by_2 = Window.partitionBy("grade_status", "course_level").orderBy(F.col("course_credits").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_ties = Window.orderBy("score")
w_course_credits = Window.partitionBy("course_credits")
w_all_by_score = Window.orderBy("score", "id")

filtered = (
    df_enrollments
    .filter((F.col("grade_status") == "WITHDRAWN") & (F.col("term").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

joined_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
        "c.course_code",
    )
)

metrics = (
    joined_course
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "course_level",
        "course_credits",
        "course_code",
        F.round(F.avg("course_credits").over(w_student_id_course_level), 2).alias("student_id_course_level_avg_course_credits"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_level_course_id), F.lit(0)), 4).alias("score_share_of_course_level_course_id"),
        F.row_number().over(w_grade_status_course_level_by).alias("rn"),
        F.nth_value("course_credits", 2).over(w_grade_status_course_level_by_2).alias("second_course_credits"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.size(F.collect_set("grade_status").over(w_course_credits)).alias("distinct_grade_status_in_course_credits"),
        F.round(F.percent_rank().over(w_all_by_score), 3).alias("score_percent_rank_2"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_course_level), 2).alias("score_vs_student_id_course_level_avg"),
    )
)

derived = (
    metrics
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_level",
        "course_credits",
        "course_code",
        "score_share_of_course_level_course_id",
        "rn",
        "second_course_credits",
        "score_percent_rank",
        "score_percent_rank_2",
        "score_vs_student_id_course_level_avg",
        (F.col("second_course_credits") + F.col("score_percent_rank_2")).alias("second_course_credits_plus_score_percent_rank_2"),
        (F.col("score") - F.col("score_percent_rank_2")).alias("score_minus_score_percent_rank_2"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "course_credits",
        "course_code",
        "score_share_of_course_level_course_id",
        "rn",
        "second_course_credits",
        "score_percent_rank_2",
        "score_vs_student_id_course_level_avg",
        "second_course_credits_plus_score_percent_rank_2",
        (F.col("score_percent_rank") + F.col("second_course_credits")).alias("score_percent_rank_plus_second_course_credits"),
        F.coalesce(F.col("score_percent_rank_2"), F.lit(0.0)).alias("score_percent_rank_2_filled"),
    )
    .orderBy(F.col("course_credits").asc(), F.col("second_course_credits").desc())
)
