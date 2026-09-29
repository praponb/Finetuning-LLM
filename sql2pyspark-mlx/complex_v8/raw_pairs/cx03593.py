from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.month(F.col("enrolled_at")).isin(1, 3)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.when(F.col("score") >= 57.8, "large").when(F.col("score") >= 39.4, "medium").otherwise("small").alias("score_tier"),
        F.when(F.col("score") >= 71.0, "large").when(F.col("score") >= 34.2, "medium").otherwise("small").alias("score_tier_2"),
    )
)

line_level = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.score_tier",
        "s.score_tier_2",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

line_level_enrollments = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.score_tier_2",
        "s.enrollment_id",
        "s.enrollment_score",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.score").alias("enrollment_score_2"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

tagged = (
    line_level_enrollments
    .filter(~F.col("enrollment_term").isin("2024T2", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "score_tier_2",
        "enrollment_id",
        "enrollment_score",
        "enrollment_id_2",
        "enrollment_term",
        "enrollment_course_id",
        "enrollment_grade_status_2",
        F.concat(F.col("enrollment_term"), F.lit("#"), F.col("enrollment_id").cast("string")).alias("enrollment_term_ref"),
    )
)

with_parent_course = (
    tagged.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.enrollment_course_id"), "inner")
    .filter(F.col("c.course_code").isNotNull())
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrollment_id",
        "s.enrollment_id_2",
        "s.enrollment_term",
        "s.enrollment_course_id",
        "s.enrollment_grade_status_2",
        "s.enrollment_term_ref",
        F.col("c.level").alias("course_level"),
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_parent_course
    .select(
        "id",
        "student_id",
        "course_id",
        "enrollment_id",
        "enrollment_id_2",
        "enrollment_term",
        "enrollment_course_id",
        "course_level",
        "course_code",
        F.coalesce(F.col("enrollment_term"), F.lit("UNKNOWN")).alias("enrollment_term_or_unknown"),
        F.concat(F.col("enrollment_term"), F.lit("#"), F.col("course_id").cast("string")).alias("enrollment_term_ref_2"),
    )
    .orderBy(
        F.col("course_id").desc(),
        F.col("enrollment_term_or_unknown").asc_nulls_last(),
    )
)
