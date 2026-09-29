from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_by_score_moving_3 = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_course_id_by_score_moving_6 = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(-6, Window.currentRow)
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_by_score_running = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_term_course_id_by_enrolled_a = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_course_id_by_score = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id")
w_course_id_by_score_full = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_course_id_by_enrolled_a_2 = Window.partitionBy("term", "course_id").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_term = Window.partitionBy("student_id", "term")
w_all_by_score_dense_rank_prio = Window.orderBy(F.col("score_dense_rank").desc(), "id").rowsBetween(-2, -1)
w_course_id_score_dense_rank_b = Window.partitionBy("course_id", "score_dense_rank").orderBy("score_vs_student_id_term_avg", "id").rowsBetween(-1, -1)
w_enrolled_at_month = Window.partitionBy(F.month("enrolled_at"))
w_course_id_score_dense_rank_b_2 = Window.partitionBy("course_id", "score_dense_rank").orderBy("score_vs_student_id_term_avg", "id")
w_course_id_score_dense_rank_b_3 = Window.partitionBy("course_id", "score_dense_rank").orderBy("score_vs_student_id_term_avg", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_dense_rank = Window.orderBy(F.col("score_dense_rank").desc(), "id")

prepared_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 20.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

ranked = (
    prepared_enrollments
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.sum("score").over(w_course_id_by_score_moving_3).alias("score_sum_4"),
        F.round(F.avg("score").over(w_course_id_by_score_moving_6), 2).alias("score_avg_7"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_term), F.lit(0)), 4).alias("score_share_of_grade_status_term"),
        F.nth_value("enrolled_at", 2).over(w_grade_status_by_enrolled_at).alias("second_enrolled_at"),
        F.max("score").over(w_course_id_by_score_running).alias("max_score_so_far"),
        F.round(F.avg("score").over(w_term_course_id_by_enrolled_a), 2).alias("score_avg_3"),
        F.row_number().over(w_course_id_by_score).alias("rn"),
        F.min("score").over(w_grade_status_term).alias("grade_status_term_min_score"),
        F.nth_value("score", 2).over(w_course_id_by_score_full).alias("second_score"),
        F.nth_value("score", 2).over(w_term_course_id_by_enrolled_a_2).alias("second_score_2"),
        F.dense_rank().over(w_course_id_by_score).alias("score_dense_rank"),
        F.max("score").over(w_student_id_term).alias("student_id_term_max_score"),
        F.sum("score").over(w_student_id_term).alias("student_id_term_sum_score"),
        F.round(F.col("score") - F.avg("score").over(w_student_id_term), 2).alias("score_vs_student_id_term_avg"),
        F.round(F.avg("score").over(w_grade_status_term), 2).alias("grade_status_term_avg_score"),
        F.count(F.lit(1)).over(w_grade_status_term).alias("rows_in_grade_status_term"),
    )
)

with_windows = (
    ranked
    .select(
        "*",
        F.max("second_score_2").over(w_all_by_score_dense_rank_prio).alias("second_score_2_max_2_prior"),
        F.sum("score_avg_7").over(w_course_id_score_dense_rank_b).alias("score_avg_7_sum_1_prior"),
        F.size(F.collect_set("course_id").over(w_enrolled_at_month)).alias("distinct_course_id_in_enrolled_at_month"),
        F.lag("score_dense_rank", 3).over(w_course_id_score_dense_rank_b_2).alias("prev3_score_dense_rank"),
        F.nth_value("second_score", 2).over(w_course_id_score_dense_rank_b_3).alias("second_second_score"),
        (F.col("score_sum_4") - F.lag("score_sum_4").over(w_all_by_score_dense_rank)).alias("score_sum_4_change"),
    )
)

result = (
    with_windows
    .filter(F.col("score_share_of_grade_status_term") <= 0.7)
    .select(
        "id",
        "course_id",
        "term",
        "score_sum_4",
        "score_share_of_grade_status_term",
        "second_enrolled_at",
        "max_score_so_far",
        "score_avg_3",
        "rn",
        "second_score_2",
        "score_dense_rank",
        "student_id_term_max_score",
        "student_id_term_sum_score",
        "score_vs_student_id_term_avg",
        "grade_status_term_avg_score",
        "rows_in_grade_status_term",
        "score_avg_7_sum_1_prior",
        "distinct_course_id_in_enrolled_at_month",
        "prev3_score_dense_rank",
        "second_second_score",
        "score_sum_4_change",
        F.date_trunc("quarter", F.col("second_enrolled_at")).alias("second_enrolled_at_quarter"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
    .orderBy("second_second_score", F.col("rows_in_grade_status_term").desc())
)
