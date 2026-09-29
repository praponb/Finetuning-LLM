from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_student_id_by_student_gpa = Window.partitionBy("student_id").orderBy(F.col("student_gpa").desc(), "id")
w_all_by_student_gpa_ties = Window.orderBy(F.col("student_gpa").desc())
w_student_id_by_student_gpa_ti = Window.partitionBy("student_id").orderBy(F.col("student_gpa").desc())

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 57.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

with_parent_course = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_len",
        F.col("c.level").alias("course_level"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

sequenced = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "student_gpa",
        F.lead("enrolled_at").over(w_student_id_by_student_gpa).alias("next_enrolled_at"),
        F.dense_rank().over(w_all_by_student_gpa_ties).alias("student_gpa_dense_rank"),
        F.dense_rank().over(w_student_id_by_student_gpa_ti).alias("student_gpa_dense_rank_2"),
        F.round(F.percent_rank().over(w_all_by_student_gpa_ties), 3).alias("student_gpa_percent_rank"),
    )
)

result = (
    sequenced
    .groupingSets(
        [["term", "student_gpa_dense_rank"], ["term"]],
        "term", "student_gpa_dense_rank",
    )
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.percentile_approx("student_gpa_percent_rank", 0.5).alias("median_student_gpa_percent_rank"),
    )
)
