from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-18 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_parent_student = (
    cleaned_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "st.student_name",
    )
)

with_children_enrollments = (
    with_parent_student.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

qualified = (
    with_children_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left_semi")
)

combined_details = (
    qualified.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .filter(F.col("st.gpa") >= 2.1)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_student_id",
        "s.enrollment_grade_status",
        "s.enrollment_enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("e.term").alias("enrollment_term_2"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrollment_id",
        "enrollment_term",
        "enrollment_student_id",
        "enrollment_grade_status",
        "enrollment_enrolled_at",
        "student_gpa",
        "student_enrolled_on",
        "enrollment_term_2",
        "enrollment_score",
    )
    .orderBy(F.col("enrollment_term").asc())
)
