from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_course_id_student_id_by_stud = Window.partitionBy("course_id", "student_id").orderBy(F.col("student_gpa").desc(), "id").rowsBetween(-6, Window.currentRow)
w_grade_status_or_unknown = Window.partitionBy("grade_status_or_unknown")
w_grade_status_or_unknown_stud = Window.partitionBy("grade_status_or_unknown", "student_cohort").orderBy("student_gpa", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_course_id_student_id_by_stud_2 = Window.partitionBy("course_id", "student_id").orderBy(F.col("student_gpa").desc(), "id")
w_course_id_student_id_by_stud_3 = Window.partitionBy("course_id", "student_id").orderBy(F.col("student_gpa").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grade_status_or_unknown_stud_2 = Window.partitionBy("grade_status_or_unknown", "student_cohort").orderBy("student_gpa", "id").rowsBetween(-6, -1)
w_term_by_student_gpa_ties = Window.partitionBy("term").orderBy("student_gpa")
w_student_id_by_enrolled_at_mo = Window.partitionBy("student_id").orderBy("enrolled_at", "id").rowsBetween(-6, Window.currentRow)
w_grade_status_or_unknown_stud_3 = Window.partitionBy("grade_status_or_unknown", "student_cohort").orderBy("student_gpa", "id")

valid = (
    df_enrollments
    .filter(F.col("term") != "2024T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

enriched = (
    valid.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") < 3.2)
    .select(
        "s.*",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        F.col("st.cohort").alias("student_cohort"),
    )
)

sequenced = (
    enriched
    .select(
        "*",
        F.max("score").over(w_course_id_student_id_by_stud).alias("score_max_7"),
        F.min("score").over(w_grade_status_or_unknown).alias("grade_status_or_unknown_min_score"),
        F.round(F.avg("student_gpa").over(w_course_id_student_id_by_stud), 2).alias("student_gpa_avg_7"),
        F.first("enrolled_at").over(w_grade_status_or_unknown_stud).alias("first_enrolled_at"),
        F.round(F.cume_dist().over(w_course_id_student_id_by_stud_2), 3).alias("student_gpa_cume_dist"),
        F.min("student_gpa").over(w_course_id_student_id_by_stud_3).alias("min_student_gpa_so_far"),
        F.sum("score").over(w_grade_status_or_unknown_stud_2).alias("score_sum_6_prior"),
        F.rank().over(w_term_by_student_gpa_ties).alias("student_gpa_rank"),
        F.round(F.avg("score").over(w_student_id_by_enrolled_at_mo), 2).alias("score_avg_7"),
        F.row_number().over(w_course_id_student_id_by_stud_2).alias("student_gpa_seq"),
        F.round(F.percent_rank().over(w_grade_status_or_unknown_stud_3), 3).alias("student_gpa_percent_rank"),
    )
)

result = (
    sequenced
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "grade_status_or_unknown",
        "term_grade_status_label",
        "student_gpa",
        "student_name",
        "student_cohort",
        "grade_status_or_unknown_min_score",
        "first_enrolled_at",
        "student_gpa_cume_dist",
        "min_student_gpa_so_far",
        "score_sum_6_prior",
        "student_gpa_rank",
        "score_avg_7",
        "student_gpa_seq",
        "student_gpa_percent_rank",
    )
    .orderBy(F.col("student_gpa_percent_rank").desc())
)
