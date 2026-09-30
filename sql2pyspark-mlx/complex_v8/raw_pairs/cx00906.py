from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 6)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

line_level_enrollments = (
    scoped_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") == "2025T1"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.days_since_enrolled_at",
        "s.grade_status_len",
        "s.term_len",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

with_details_enrollment = (
    line_level_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrollment_id",
        "s.enrollment_enrolled_at",
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_details_enrollment
    .rollup("enrollment_id", "enrollment_student_id", "course_id")
    .agg(
        F.count(F.when(~F.col("student_is_international"), 1)).alias("n_not_student_is_international"),
    )
)
