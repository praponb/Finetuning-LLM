from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("score") <= 66.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

qualified = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

combined_details_student = (
    qualified.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.is_international")), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

filtered = (
    combined_details_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (~F.col("e.grade_status").isin("FAIL", "PASS")), "left_anti")
    .filter(F.col("score").between(30.0, 73.7))
)

filtered_2 = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")), "left_anti")
    .filter(F.col("score") <= 59.8)
)

result = (
    filtered_2
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "student_gpa",
        "student_cohort",
        "course_level",
        "course_dept",
        F.greatest(F.col("score"), F.col("student_gpa")).alias("greatest_score_student_gpa"),
    )
)
