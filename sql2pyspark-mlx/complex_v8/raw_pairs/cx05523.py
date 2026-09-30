from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter((F.col("student_id") >= 18) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 59.7).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled
    .rollup("enrollment_count", "grade_status", "student_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("enrolled_at").alias("first_enrolled_at"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
    )
    .select(
        "enrollment_count",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "student_id",
        "grouping_level",
        "first_enrolled_at",
        "distinct_student_ids",
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
