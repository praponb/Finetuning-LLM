from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("score") <= 57.6) & (F.month(F.col("enrolled_at")).isin(2, 3)))
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

with_parent_student = (
    scoped.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") >= 2.0)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

screened = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

result = (
    screened
    .cube("term", "student_id", "student_is_international")
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
)
