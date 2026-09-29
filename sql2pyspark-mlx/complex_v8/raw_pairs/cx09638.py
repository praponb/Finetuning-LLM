from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_student_id_by_s = Window.partitionBy("grade_status", "student_id").orderBy("score", "id")
w_course_id_term = Window.partitionBy("course_id", "term")
w_student_id = Window.partitionBy("student_id")
w_student_id_grade_status_by_s = Window.partitionBy("student_id", "grade_status").orderBy(F.col("score").desc(), "id")
w_course_id_by_score_full = Window.partitionBy("course_id").orderBy(F.col("score").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_term_grade_status_by_score_f = Window.partitionBy("term", "grade_status").orderBy(F.col("score_filled").desc(), "id")
w_term_grade_status_by_score_f_2 = Window.partitionBy("term", "grade_status").orderBy(F.col("score_filled").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_distinct_course_id_in_studen = Window.partitionBy("distinct_course_id_in_student_id").orderBy(F.col("score_dense_rank").desc(), "id")
w_grade_status_rn_by_distinct = Window.partitionBy("grade_status", "rn").orderBy(F.col("distinct_course_id_in_student_id").asc_nulls_last(), "id")
w_grade_status_rn_by_distinct_2 = Window.partitionBy("grade_status", "rn").orderBy(F.col("distinct_course_id_in_student_id").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN", "PASS")) & (F.col("score").between(8.7, 66.0)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

sequenced = (
    cleaned_enrollments
    .select(
        "*",
        F.dense_rank().over(w_grade_status_student_id_by_s).alias("score_dense_rank"),
        F.count(F.lit(1)).over(w_course_id_term).alias("rows_in_course_id_term"),
        F.max("score").over(w_student_id).alias("student_id_max_score"),
        F.row_number().over(w_student_id_grade_status_by_s).alias("rn"),
        F.last("score_filled").over(w_course_id_by_score_full).alias("last_score_filled"),
        F.size(F.collect_set("course_id").over(w_student_id)).alias("distinct_course_id_in_student_id"),
        F.round(F.col("score_filled") - F.avg("score_filled").over(w_course_id_term), 2).alias("score_filled_vs_course_id_term_avg"),
        F.row_number().over(w_term_grade_status_by_score_f).alias("rn_2"),
        F.sum("score_filled").over(w_term_grade_status_by_score_f_2).alias("running_score_filled"),
    )
)

sequenced_2 = (
    sequenced
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "score_filled",
        "rows_in_course_id_term",
        "student_id_max_score",
        "last_score_filled",
        "score_filled_vs_course_id_term_avg",
        "rn_2",
        "running_score_filled",
        F.row_number().over(w_distinct_course_id_in_studen).alias("seq_no"),
        F.dense_rank().over(w_distinct_course_id_in_studen).alias("score_dense_rank_dense_rank"),
        F.sum("last_score_filled").over(w_student_id).alias("student_id_sum_last_score_filled"),
        F.dense_rank().over(w_grade_status_rn_by_distinct).alias("distinct_course_id_in_student_id_dense_rank"),
        F.nth_value("student_id_max_score", 2).over(w_grade_status_rn_by_distinct_2).alias("second_student_id_max_score"),
        F.sum("score").over(w_student_id).alias("student_id_sum_score"),
        F.max("score_filled").over(w_student_id).alias("student_id_max_score_filled"),
        F.count(F.lit(1)).over(w_student_id).alias("rows_in_student_id"),
    )
)

result = (
    sequenced_2
    .filter(F.col("score_filled_vs_course_id_term_avg").between(-60.0, 90.0))
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "score_filled",
        "rows_in_course_id_term",
        "score_filled_vs_course_id_term_avg",
        "rn_2",
        "running_score_filled",
        "seq_no",
        "score_dense_rank_dense_rank",
        "distinct_course_id_in_student_id_dense_rank",
        "second_student_id_max_score",
        "student_id_sum_score",
        "student_id_max_score_filled",
        "rows_in_student_id",
    )
    .orderBy("student_id_sum_score")
)
