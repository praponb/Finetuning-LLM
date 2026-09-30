from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_enrolled_at_year_term_by_sco = Window.partitionBy(F.year("enrolled_at"), "term").orderBy(F.col("score_distance").asc_nulls_last(), "id").rowsBetween(-3, -1)
w_course_id_student_id_by_enro = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id = Window.partitionBy("course_id")
w_term_student_id_by_score_tie = Window.partitionBy("term", "student_id").orderBy("score")
w_course_id_student_id_by_enro_2 = Window.partitionBy("course_id", "student_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

valid_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 38.1).alias("score_distance"),
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

windowed = (
    valid_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "score_adj",
        F.sum("score_distance").over(w_enrolled_at_year_term_by_sco).alias("score_distance_sum_3_prior"),
        F.first("enrolled_at", ignorenulls=True).over(w_course_id_student_id_by_enro).alias("first_known_enrolled_at"),
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.min("score").over(w_course_id).alias("course_id_min_score"),
        F.round(F.percent_rank().over(w_term_student_id_by_score_tie), 3).alias("score_percent_rank"),
        F.max("score_distance").over(w_course_id).alias("course_id_max_score_distance"),
        F.nth_value("score", 2).over(w_course_id_student_id_by_enro_2).alias("second_score"),
        F.dense_rank().over(w_term_student_id_by_score_tie).alias("score_dense_rank"),
    )
)

top_rows = windowed.filter(F.col("score_dense_rank") == 1)

result = (
    top_rows
    .filter(F.hour(F.col("first_known_enrolled_at")).between(8, 12))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "score_adj",
        "score_distance_sum_3_prior",
        "first_known_enrolled_at",
        "rows_in_course_id",
        "course_id_min_score",
        "score_percent_rank",
        "course_id_max_score_distance",
        "score_dense_rank",
        F.round(F.col("score_percent_rank") / F.nullif(F.col("score_adj"), F.lit(0)), 3).alias("score_percent_rank_per_score_adj"),
        F.substring(F.trim(F.col("grade_status")), 1, 4).alias("grade_status_prefix"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
