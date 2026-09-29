from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter((F.month(F.col("enrolled_at")).isin(1, 2)) & (F.col("student_id") <= 10))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
    )
)

combined_details_course = (
    cleaned.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status",
        "s.enrolled_at",
        "s.enrolled_at_date",
        "s.term_ref",
        F.col("c.dept").alias("course_dept"),
        "c.course_code",
    )
)

joined = (
    combined_details_course.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.col("c.credits") < 3)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.enrolled_at_date",
        "s.term_ref",
        "s.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    joined
    .filter(F.col("student_id").isNotNull())
    .select("student_id")
    .distinct()
    .orderBy(F.col("student_id").desc())
)
