from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

line_level = (
    scoped.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.score_bucket",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

enrollment_grade_status_stats = (
    line_level
    .groupBy("enrollment_grade_status")
    .agg(F.max("score_bucket").alias("max_score_bucket"))
)

compared = (
    line_level.alias("s")
    .join(enrollment_grade_status_stats.alias("g"), F.col("g.enrollment_grade_status") == F.col("s.enrollment_grade_status"), "inner")
    .filter(F.col("s.score_bucket") == F.col("g.max_score_bucket"))
    .select(
        "s.*",
        "g.max_score_bucket",
        F.round(F.col("s.score_bucket") - F.col("g.max_score_bucket"), 2).alias("score_bucket_minus_max"),
    )
)

result = (
    compared
    .groupingSets(
        [["enrollment_grade_status", "enrollment_term"], ["enrollment_grade_status"], ["enrollment_term"], []],
        "enrollment_grade_status", "enrollment_term",
    )
    .agg(
        F.grouping("enrollment_grade_status").alias("is_total_enrollment_grade_status"),
        F.round(100 * F.count(F.when(F.col("enrollment_grade_status") == "WITHDRAWN", 1)) / F.count("*"), 1).alias("pct_enrollment_grade_status_with"),
    )
    .orderBy(
        F.col("enrollment_grade_status").asc_nulls_last(),
        F.col("enrollment_term").asc_nulls_last(),
    )
)
