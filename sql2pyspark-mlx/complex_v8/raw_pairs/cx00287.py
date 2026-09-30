from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_term_grade_status_by_score = Window.partitionBy("term", "grade_status").orderBy(F.col("score").desc(), "id")
w_student_id = Window.partitionBy("student_id")
w_course_id_by_score = Window.partitionBy("course_id").orderBy("score", "id")
w_student_id_by_score = Window.partitionBy("student_id").orderBy("score", "id")
w_term_grade_status_by_score_m = Window.partitionBy("term", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-6, Window.currentRow)
w_course_id_by_score_full = Window.partitionBy("course_id").orderBy("score", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_student_id_by_score_ties = Window.partitionBy("student_id").orderBy("score")
w_term_grade_status_by_score_m_2 = Window.partitionBy("term", "grade_status").orderBy(F.col("score").desc(), "id").rowsBetween(-1, Window.currentRow)
w_student_id_by_score_moving_1 = Window.partitionBy("student_id").orderBy("score", "id").rowsBetween(-1, Window.currentRow)
w_all_by_prev3_score = Window.orderBy("prev3_score", "id")
w_score_rank = Window.partitionBy("score_rank")

base = (
    df_enrollments
    .filter((F.col("course_id") <= 5) | (F.col("enrolled_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

ranked = (
    base
    .select(
        "*",
        F.lag("score").over(w_term_grade_status_by_score).alias("prev_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_student_id), F.lit(0)), 4).alias("score_share_of_student_id"),
        F.size(F.collect_set("term").over(w_student_id)).alias("distinct_term_in_student_id"),
        (F.col("score") - F.lag("score").over(w_course_id_by_score)).alias("score_change"),
        F.lag("score", 2).over(w_student_id_by_score).alias("prev2_score"),
        F.max("score").over(w_term_grade_status_by_score_m).alias("score_max_7"),
        F.nth_value("score", 2).over(w_course_id_by_score_full).alias("second_score"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
        F.min("score").over(w_student_id).alias("student_id_min_score"),
        F.round(F.cume_dist().over(w_student_id_by_score_ties), 3).alias("score_cume_dist"),
        F.round(F.avg("score").over(w_term_grade_status_by_score_m_2), 2).alias("score_avg_2"),
        F.rank().over(w_student_id_by_score).alias("score_rank"),
        F.lag("score", 3).over(w_course_id_by_score).alias("prev3_score"),
        F.round(F.avg("score").over(w_student_id), 2).alias("student_id_avg_score"),
        F.sum("score").over(w_student_id_by_score_moving_1).alias("score_sum_2"),
    )
)

first_rows = ranked.filter(F.col("score_rank") <= 5)

with_windows = (
    first_rows
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "prev_score",
        "score_share_of_student_id",
        "score_change",
        "score_max_7",
        "second_score",
        "rows_in_student_id",
        "student_id_min_score",
        "score_cume_dist",
        "score_avg_2",
        "prev3_score",
        "student_id_avg_score",
        "score_sum_2",
        (F.col("distinct_term_in_student_id") - F.lag("distinct_term_in_student_id").over(w_all_by_prev3_score)).alias("distinct_term_in_student_id_change"),
        F.count(F.lit(1)).over(w_score_rank).alias("rows_in_score_rank"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "score_share_of_student_id",
        "score_change",
        "second_score",
        "rows_in_student_id",
        "student_id_min_score",
        "score_cume_dist",
        "prev3_score",
        "student_id_avg_score",
        "score_sum_2",
        "distinct_term_in_student_id_change",
        "rows_in_score_rank",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("rows_in_score_rank").cast("string")).alias("grade_status_ref"),
    )
    .orderBy(F.col("score_change").desc())
)
