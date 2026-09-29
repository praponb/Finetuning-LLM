from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("course_id") >= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

enriched = (
    base_enrollments
    .select(
        "*",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date_2"),
        F.abs(F.col("score_bucket") - 35).alias("score_bucket_distance"),
        F.length(F.trim(F.col("grade_status_or_unknown"))).alias("grade_status_or_unknown_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_counts = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .filter(F.col("score_bucket") >= 20)
    .select(
        "id",
        "student_id",
        "grade_status",
        "enrolled_at",
        "grade_status_or_unknown",
        "score_bucket",
        "enrolled_at_date",
        "enrolled_at_date_2",
        "score_bucket_distance",
        "grade_status_or_unknown_len",
        F.date_format(F.col("enrolled_at_date_2"), "yyyy-MM").alias("enrolled_at_date_2_label"),
    )
    .orderBy(F.col("enrolled_at_date_2").asc())
)
