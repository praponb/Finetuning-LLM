from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.5, 2).alias("score_adj"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

course_id_stats = (
    cleaned_enrollments
    .groupBy("course_id")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

outliers = (
    cleaned_enrollments.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

student_id_stats = (
    outliers
    .groupBy("student_id")
    .agg(F.max("median_score").alias("max_median_score"))
)

outliers_2 = (
    outliers.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.median_score") == F.col("g.max_median_score"))
    .select(
        "s.*",
        "g.max_median_score",
        F.round(F.col("s.median_score") - F.col("g.max_median_score"), 2).alias("median_score_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
    )
)

summarized_rows = (
    outliers_2.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.max_enrollment_score")
)

result = (
    summarized_rows
    .groupBy("course_id", "last_enrollment_enrolled_at")
    .pivot("grade_status_or_unknown", ["PASS", "INCOMPLETE", "FAIL"])
    .agg(F.avg("score_adj"))
    .orderBy("course_id", "last_enrollment_enrolled_at")
)
