from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

profiled = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

with_children = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.enrolled_at_hour",
        "s.score_filled",
        "s.first_enrollment_enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    with_children
    .cube("student_id", "enrolled_at_hour", "enrollment_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.sum(F.when(F.col("enrollment_term").isin("2025T2", "2024T1"), F.col("score"))).alias("score_enrollment_term_in_2025t2_20"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("enrolled_at_hour").asc_nulls_last(),
        F.col("enrollment_id").asc_nulls_last(),
    )
)
