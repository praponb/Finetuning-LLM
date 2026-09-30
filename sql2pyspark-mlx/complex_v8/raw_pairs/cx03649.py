from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") < F.lit("2025-02-12 12:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

tagged = (
    prepared_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

line_level_enrollments = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.term_prefix",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

course_id_stats = (
    line_level_enrollments
    .groupBy("course_id")
    .agg(F.percentile_approx("enrollment_score", 0.5).alias("median_enrollment_score"))
)

outliers = (
    line_level_enrollments.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.enrollment_score") >= F.col("g.median_enrollment_score"))
    .select(
        "s.*",
        "g.median_enrollment_score",
        F.round(F.col("s.enrollment_score") - F.col("g.median_enrollment_score"), 2).alias("enrollment_score_minus_median"),
    )
)

without_matches = (
    outliers.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id").isNotNull()) & (F.hour(F.col("e.enrolled_at")).between(10, 13)), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "course_id",
        "term",
        "term_prefix",
        "enrollment_id",
        "enrollment_student_id",
        "median_enrollment_score",
        F.substring(F.trim(F.col("term_prefix")), 1, 2).alias("term_prefix_prefix"),
        F.round(F.col("median_enrollment_score") * 0.9, 2).alias("median_enrollment_score_adj"),
    )
    .orderBy(F.col("id").desc(), "median_enrollment_score_adj")
)
