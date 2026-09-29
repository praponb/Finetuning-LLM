from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "FAIL") & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enriched = (
    with_stats
    .select(
        "*",
        F.greatest(F.col("score"), F.col("max_enrollment_score")).alias("greatest_score_max_enrollment_score"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count("*").alias("enrollment_count_2"),
    )
)

profiled = (
    enriched.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

result = (
    profiled
    .groupBy("course_id")
    .pivot("grade_status", ["PASS", "FAIL"])
    .agg(F.max("enrollment_count_2"))
    .orderBy("course_id")
)
