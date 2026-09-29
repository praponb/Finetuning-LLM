from pyspark.sql import functions as F

cleaned_courses = (
    df_courses
    .filter((F.col("credits") >= 2) | (F.col("dept").isNotNull()))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
    )
)

exploded_detail = (
    cleaned_courses.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.grade_status").isin("INCOMPLETE", "FAIL", "WITHDRAWN")), "inner")
    .select(
        "s.id",
        "s.course_code",
        "s.credits",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

with_parent_enrollment = (
    exploded_detail.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("e.student_id"), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.credits",
        "s.enrollment_id",
        "s.enrollment_grade_status",
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

derived = (
    with_parent_enrollment
    .select(
        "*",
        F.abs(F.col("credits") - 3).alias("credits_distance"),
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.substring(F.trim(F.col("enrollment_grade_status")), 1, 2).alias("enrollment_grade_status_prefix"),
    )
)

screened = (
    derived.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "left_semi")
)

derived_2 = (
    screened
    .filter(F.upper(F.trim(F.col("course_code"))).like("A%"))
    .withColumn("credits_filled_per_credits_distance", F.round(F.col("credits_filled") / F.nullif(F.col("credits_distance"), F.lit(0)), 3))
)

result = (
    derived_2
    .filter(F.col("enrollment_course_id") >= 7)
    .select(
        "id",
        "course_code",
        "enrollment_grade_status",
        "enrollment_course_id",
        "student_is_international",
        "credits_distance",
        "enrollment_grade_status_prefix",
        "credits_filled_per_credits_distance",
    )
    .orderBy(F.col("credits_distance").desc())
)
