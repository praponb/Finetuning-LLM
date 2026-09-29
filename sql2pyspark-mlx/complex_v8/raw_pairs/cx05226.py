from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

detail_rows = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.days_since_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.score").alias("enrollment_score"),
    )
)

joined_course = (
    detail_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.days_since_enrolled_at",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_score",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    joined_course
    .filter(F.col("enrollment_student_id") <= 9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "days_since_enrolled_at",
        "enrollment_id",
        "enrollment_student_id",
        "course_level",
        F.concat(F.col("course_level"), F.lit("#"), F.col("course_id").cast("string")).alias("course_level_ref"),
        F.round(F.col("enrollment_score") / F.nullif(F.col("days_since_enrolled_at"), F.lit(0)), 3).alias("enrollment_score_per_days_since_enrolled_at"),
    )
    .orderBy(F.col("id").desc_nulls_first(), F.col("days_since_enrolled_at").asc())
)
