from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id = Window.partitionBy("course_id")
w_all_by_score = Window.orderBy(F.col("score").desc(), "id")
w_student_id_by_enrolled_at_fu = Window.partitionBy("student_id").orderBy(F.col("enrolled_at").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_by_score_full = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_score_running = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_course_id_student_id = Window.partitionBy("course_id", "student_id")
w_term_by_score = Window.partitionBy("term").orderBy(F.col("score").asc_nulls_last(), "id")

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 13:00:00").cast("timestamp"))
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

with_windows = (
    cleaned_enrollments
    .select(
        "*",
        F.count(F.lit(1)).over(w_course_id).alias("rows_in_course_id"),
        F.row_number().over(w_all_by_score).alias("rn"),
        F.lag("enrolled_at", 2).over(w_all_by_score).alias("prev2_enrolled_at"),
        F.first("enrolled_at").over(w_student_id_by_enrolled_at_fu).alias("first_enrolled_at"),
        F.first("score").over(w_term_by_score_full).alias("first_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id), 2).alias("score_vs_course_id_avg"),
        F.first("score", ignorenulls=True).over(w_all_by_score_running).alias("first_known_score"),
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_course_id), F.lit(0)), 4).alias("score_share_of_course_id"),
        F.nth_value("enrolled_at", 2).over(w_student_id_by_enrolled_at_fu).alias("second_enrolled_at"),
        F.size(F.collect_set("grade_status").over(w_course_id_student_id)).alias("distinct_grade_status_in_course_id_student_id"),
        F.lag("score").over(w_term_by_score).alias("prev_score"),
        F.ntile(10).over(w_term_by_score).alias("score_decile"),
        F.nth_value("enrolled_at", 2).over(w_term_by_score_full).alias("second_enrolled_at_2"),
        F.count(F.lit(1)).over(w_course_id_student_id).alias("rows_in_course_id_student_id"),
        F.round(F.avg("score").over(w_course_id_student_id), 2).alias("course_id_student_id_avg_score"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "rows_in_course_id",
        "rn",
        "prev2_enrolled_at",
        "first_enrolled_at",
        "first_score",
        "first_known_score",
        "prev_score",
        "second_enrolled_at_2",
        "rows_in_course_id_student_id",
        "course_id_student_id_avg_score",
    )
)
