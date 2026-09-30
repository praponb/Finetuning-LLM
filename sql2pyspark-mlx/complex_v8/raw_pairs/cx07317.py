from pyspark.sql import functions as F

scoped_courses = (
    df_courses
    .filter(F.col("credits").between(2, 4))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
        F.lower(F.trim(F.col("level"))).alias("level_clean"),
    )
)

detail_rows_enrollments = (
    scoped_courses.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        "s.level",
        "s.credits_filled",
        "s.level_clean",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enrollment_student_id_stats = (
    detail_rows_enrollments
    .groupBy("enrollment_student_id")
    .agg(F.percentile_approx("credits_filled", 0.5).alias("median_credits_filled"))
)

compared = (
    detail_rows_enrollments.alias("s")
    .join(enrollment_student_id_stats.alias("g"), F.col("g.enrollment_student_id") == F.col("s.enrollment_student_id"), "inner")
    .filter(F.col("s.credits_filled") >= F.col("g.median_credits_filled"))
    .select(
        "s.*",
        "g.median_credits_filled",
        F.round(F.col("s.credits_filled") - F.col("g.median_credits_filled"), 2).alias("credits_filled_minus_median"),
    )
)

result = (
    compared
    .select(
        "id",
        "level",
        "credits_filled",
        "enrollment_id",
        "enrollment_term",
        "median_credits_filled",
        "credits_filled_minus_median",
        F.date_trunc("hour", F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_hour"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
