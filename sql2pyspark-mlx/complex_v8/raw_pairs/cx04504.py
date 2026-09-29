from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status") != "WITHDRAWN", 1)).alias("enrollment_grade_status_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    scoped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.first_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

qualified = (
    profiled
    .crossJoin(profiled.agg(F.max("enrollment_count").alias("overall_max_enrollment_count")))
    .filter(F.col("enrollment_count") < F.col("overall_max_enrollment_count"))
    .drop("overall_max_enrollment_count")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

summarized_rows = (
    qualified.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

result = (
    summarized_rows
    .filter(F.col("term") != "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at_dayofweek",
        "enrollment_grade_status_count",
        "first_enrollment_enrolled_at",
        "enrollment_count",
        "avg_enrollment_score",
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)
