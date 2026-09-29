from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term = Window.partitionBy("term")
w_grade_status_by_enrolled_at = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc())
w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_all_by_score_full = Window.orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status_by_score_f = Window.partitionBy("term", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_student_id_by_score_run = Window.partitionBy("term", "student_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_by_enrolled_at_2 = Window.partitionBy("grade_status").orderBy(F.col("enrolled_at").desc(), "id").rowsBetween(-1, -1)
w_term_grade_status_by_score = Window.partitionBy("term", "grade_status").orderBy(F.col("score").desc(), "id")
w_all_by_score_moving_2 = Window.orderBy("score", "id").rowsBetween(-2, Window.currentRow)
w_term_student_id_by_score = Window.partitionBy("term", "student_id").orderBy("score", "id")
w_all_by_score_moving_1 = Window.orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_rn_2 = Window.partitionBy("rn_2")
w_all_by_second_score = Window.orderBy("second_score", "id")
w_course_id_by_second_score = Window.partitionBy("course_id").orderBy("second_score", "id")

prepared = (
    df_enrollments
    .filter((F.col("score") >= 5.9) | (F.col("term").isNotNull()))
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

windowed = (
    prepared
    .select(
        "*",
        F.round(F.col("score") - F.avg("score").over(w_term), 2).alias("score_vs_term_avg"),
        F.rank().over(w_grade_status_by_enrolled_at).alias("enrolled_at_rank"),
        F.round(F.col("score") - F.avg("score").over(w_grade_status_course_id), 2).alias("score_vs_grade_status_course_id_avg"),
        F.first("score").over(w_all_by_score_full).alias("first_score"),
        F.nth_value("score", 2).over(w_term_grade_status_by_score_f).alias("second_score"),
        F.count(F.lit(1)).over(w_term_student_id_by_score_run).alias("running_count"),
        F.max("score").over(w_grade_status_by_enrolled_at_2).alias("score_max_1_prior"),
        F.row_number().over(w_term_grade_status_by_score).alias("rn"),
        F.sum("score").over(w_all_by_score_moving_2).alias("score_sum_3"),
        F.max("score").over(w_term).alias("term_max_score"),
        F.row_number().over(w_term_student_id_by_score).alias("rn_2"),
        F.round(F.avg("score").over(w_all_by_score_moving_1), 2).alias("score_avg_2"),
    )
)

top_per_group = (
    windowed
    .filter((F.col("enrolled_at_rank") <= 2) & (F.col("score_vs_grade_status_course_id_avg") >= -80.0))
    .drop("enrolled_at_rank")
)

with_windows = (
    top_per_group
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "score_vs_term_avg",
        "first_score",
        "second_score",
        "score_max_1_prior",
        "term_max_score",
        "rn_2",
        "score_avg_2",
        F.round(F.col("first_score") - F.avg("first_score").over(w_rn_2), 2).alias("first_score_vs_rn_2_avg"),
        F.count(F.lit(1)).over(w_rn_2).alias("rows_in_rn_2"),
        F.lag("score_vs_term_avg", 3).over(w_all_by_second_score).alias("prev3_score_vs_term_avg"),
        F.rank().over(w_course_id_by_second_score).alias("second_score_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "first_score",
        "second_score",
        "score_max_1_prior",
        "term_max_score",
        "rn_2",
        "score_avg_2",
        "first_score_vs_rn_2_avg",
        "rows_in_rn_2",
        "prev3_score_vs_term_avg",
        "second_score_rank",
        F.round(F.col("score_max_1_prior") * 0.9, 2).alias("score_max_1_prior_adj"),
    )
)
