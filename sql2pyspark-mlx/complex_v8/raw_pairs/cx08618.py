from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("student_id") <= 19)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

screened = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term").isNotNull()), "left_semi")
    .filter(F.col("student_id").isNotNull())
)

combined_details = (
    screened.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.enrolled_at_month",
        "s.grade_status_slug",
        "c.course_code",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at_month",
        "grade_status_slug",
        "course_code",
        "student_is_international",
        (F.col("student_gpa") - F.col("enrolled_at_month")).alias("student_gpa_minus_enrolled_at_month"),
        F.coalesce(F.col("enrolled_at_month"), F.lit(0)).alias("enrolled_at_month_filled"),
    )
    .orderBy(F.col("enrolled_at_month").desc())
)
