from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.date_trunc("month", F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

tagged = (
    base
    .select(
        "*",
        F.round(F.col("score") / F.nullif(F.col("score_bucket"), F.lit(0)), 3).alias("score_per_score_bucket"),
        F.coalesce(F.col("score_bucket"), F.lit(0)).alias("score_bucket_filled"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["term", "enrollment_count", "grade_status"], ["term"], []],
        "term", "enrollment_count", "grade_status",
    )
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("enrollment_count").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
