from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

line_level = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enriched = (
    line_level
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "enrollment_id",
        "enrollment_course_id",
        "enrollment_enrolled_at",
        F.year(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_year"),
        F.to_date(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_date"),
    )
)

combined_details_student = (
    enriched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.enrollment_id",
        "s.enrollment_course_id",
        "s.enrollment_enrolled_at",
        "s.enrollment_enrolled_at_year",
        "s.enrollment_enrolled_at_date",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "enrollment_course_id",
        "enrollment_enrolled_at_year",
        "enrollment_enrolled_at_date",
        "student_gpa",
        "student_is_international",
        F.round((F.unix_timestamp(F.col("enrolled_at")) - F.unix_timestamp(F.col("enrollment_enrolled_at"))) / 3600, 1).alias("hours_enrollment_enrolled_at_to_enrolled_at"),
    )
    .orderBy(F.col("enrollment_course_id").desc())
)
