from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

combined_details_student = (
    filtered_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
    )
)

result = (
    combined_details_student
    .filter(F.col("student_gpa") <= 3.2)
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "student_gpa",
        "student_is_international",
        "student_name",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.coalesce(F.col("student_gpa"), F.lit(0.0)).alias("student_gpa_filled"),
    )
    .orderBy(F.col("student_gpa_filled").asc_nulls_last())
)
