from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_term_by_score_ties = Window.partitionBy("course_id", "term").orderBy("score")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_student_id_course_id_by_scor = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_prior_3 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-3, -1)
w_course_id_term_by_score = Window.partitionBy("course_id", "term").orderBy("score", "id")
w_all_by_score_moving_1 = Window.orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_grade_status = Window.partitionBy("grade_status")
w_student_id_course_id_by_scor_2 = Window.partitionBy("student_id", "course_id").orderBy(F.col("score").desc())
w_grade_status_term = Window.partitionBy("grade_status", "term")
w_distinct_course_id_in_grade = Window.partitionBy("distinct_course_id_in_grade_status", "student_id").orderBy(F.col("score_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_course_id_in_grade_2 = Window.partitionBy("distinct_course_id_in_grade_status", "student_id").orderBy(F.col("score_distance").desc(), "id")
w_course_id = Window.partitionBy("course_id")

prepared = (
    df_enrollments
    .filter((F.col("score") < 80.2) | (F.col("grade_status") == "PASS"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat_ws(": ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

with_trends = (
    prepared
    .select(
        "*",
        F.round(F.percent_rank().over(w_course_id_term_by_score_ties), 3).alias("score_percent_rank"),
        F.lag("score", 2).over(w_all_by_score).alias("prev2_score"),
        F.nth_value("score", 2).over(w_student_id_course_id_by_scor).alias("second_score"),
        F.max("score").over(w_all_by_score_prior_3).alias("score_max_3_prior"),
        F.lead("score", 1, 0).over(w_course_id_term_by_score).alias("next_score"),
        F.max("score").over(w_all_by_score_moving_1).alias("score_max_2"),
        F.ntile(3).over(w_all_by_score).alias("score_tercile"),
        F.size(F.collect_set("course_id").over(w_grade_status)).alias("distinct_course_id_in_grade_status"),
        F.rank().over(w_student_id_course_id_by_scor_2).alias("score_rank"),
        F.count(F.lit(1)).over(w_grade_status_term).alias("rows_in_grade_status_term"),
        F.sum("score").over(w_grade_status).alias("grade_status_sum_score"),
    )
)

scored = (
    with_trends
    .filter(F.col("second_score") > 20.2)
    .select(
        "*",
        F.greatest(F.col("prev2_score"), F.col("score_percent_rank")).alias("greatest_prev2_score_score_percent_rank"),
        F.abs(F.col("score") - 69.6).alias("score_distance"),
        (F.floor(F.col("grade_status_sum_score") / 250) * 250).alias("grade_status_sum_score_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.min("grade_status_sum_score").over(w_distinct_course_id_in_grade).alias("min_grade_status_sum_score_so_far"),
        (F.col("second_score") - F.lag("second_score").over(w_distinct_course_id_in_grade_2)).alias("second_score_change"),
        F.sum("second_score").over(w_course_id).alias("course_id_sum_second_score"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "term_grade_status_label",
        "score_percent_rank",
        "prev2_score",
        "second_score",
        "next_score",
        "score_max_2",
        "score_tercile",
        "score_rank",
        "rows_in_grade_status_term",
        "grade_status_sum_score",
        "greatest_prev2_score_score_percent_rank",
        "score_distance",
        "grade_status_sum_score_bucket",
        "min_grade_status_sum_score_so_far",
        "second_score_change",
        "course_id_sum_second_score",
    )
    .orderBy(F.col("grade_status").desc())
)
