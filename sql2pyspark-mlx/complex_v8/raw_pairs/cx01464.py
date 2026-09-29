from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("course_id") <= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 74.9, "A").when(F.col("score") >= 20.6, "B").otherwise("C").alias("score_tier"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

with_details_course = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.*",
        "c.course_code",
        F.col("c.level").alias("course_level"),
        F.col("c.credits").alias("course_credits"),
    )
)

line_level = (
    with_details_course.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.grade_status_ref",
        "s.course_credits",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

result = (
    line_level
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "grade_status_ref",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_term",
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
    )
    .orderBy(F.col("course_id").desc())
)
