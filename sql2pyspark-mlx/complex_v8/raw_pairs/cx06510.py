from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grade_status_course_id = Window.partitionBy("grade_status", "course_id")
w_grade_status_by_student_enro = Window.partitionBy("grade_status").orderBy("student_enrolled_on", "id")
w_term = Window.partitionBy("term")
w_all_by_score_full = Window.orderBy(F.col("score").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_term = Window.partitionBy("course_id", "term")
w_student_id_term_by_student_e = Window.partitionBy("student_id", "term").orderBy("student_enrolled_on", "id")
w_all_by_score_prior_6 = Window.orderBy("score", "id").rowsBetween(-6, -1)
w_all_by_score = Window.orderBy("score", "id")
w_all_by_score_ties = Window.orderBy("score")
w_grade_status_by_student_enro_2 = Window.partitionBy("grade_status").orderBy("student_enrolled_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid_enrollments = (
    df_enrollments
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

combined_details = (
    valid_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

with_windows = (
    combined_details
    .select(
        "id",
        "course_id",
        "term",
        "grade_status",
        "student_enrolled_on",
        F.round(F.col("score") / F.nullif(F.sum("score").over(w_grade_status_course_id), F.lit(0)), 4).alias("score_share_of_grade_status_course_id"),
        F.lag("score").over(w_grade_status_by_student_enro).alias("prev_score"),
        F.size(F.collect_set("course_id").over(w_term)).alias("distinct_course_id_in_term"),
        F.nth_value("student_enrolled_on", 2).over(w_all_by_score_full).alias("second_student_enrolled_on"),
        F.count(F.lit(1)).over(w_grade_status_course_id).alias("rows_in_grade_status_course_id"),
        F.lead("score", 1, 0).over(w_grade_status_by_student_enro).alias("next_score"),
        F.round(F.col("score") - F.avg("score").over(w_course_id_term), 2).alias("score_vs_course_id_term_avg"),
        F.dense_rank().over(w_grade_status_by_student_enro).alias("student_enrolled_on_dense_rank"),
        F.ntile(10).over(w_student_id_term_by_student_e).alias("student_enrolled_on_decile"),
        F.last("score").over(w_all_by_score_full).alias("last_score"),
        F.round(F.avg("score").over(w_all_by_score_prior_6), 2).alias("score_avg_6_prior"),
        F.row_number().over(w_all_by_score).alias("seq_no"),
        F.round(F.percent_rank().over(w_all_by_score_ties), 3).alias("score_percent_rank"),
        F.size(F.collect_set("grade_status").over(w_term)).alias("distinct_grade_status_in_term"),
        F.first("student_enrolled_on", ignorenulls=True).over(w_grade_status_by_student_enro_2).alias("first_known_student_enrolled_on"),
    )
)

result = (
    with_windows
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "term",
        "student_enrolled_on",
        "prev_score",
        "distinct_course_id_in_term",
        "second_student_enrolled_on",
        "rows_in_grade_status_course_id",
        "next_score",
        "score_vs_course_id_term_avg",
        "student_enrolled_on_dense_rank",
        "last_score",
        "score_avg_6_prior",
        "seq_no",
        "score_percent_rank",
        "distinct_grade_status_in_term",
        "first_known_student_enrolled_on",
    )
    .orderBy("seq_no")
)
