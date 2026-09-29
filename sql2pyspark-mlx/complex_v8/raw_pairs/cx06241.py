from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-24 08:00:00").cast("timestamp")) & (F.col("student_id") <= 2))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

with_details_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.credits") > 2), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("c.credits") > 2)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.score_filled",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

exploded_detail = (
    with_details_course.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") <= 58.8), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.student_major",
        "s.student_cohort",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "student_id",
        "course_id",
        "student_major",
        "student_cohort",
        "enrollment_id",
        "enrollment_score",
        "enrollment_enrolled_at",
        "enrollment_course_id",
        F.date_trunc("quarter", F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_quarter"),
        F.coalesce(F.col("student_major"), F.lit("UNKNOWN")).alias("student_major_or_unknown"),
    )
    .orderBy(F.col("enrollment_id").desc())
)
