from pyspark.sql import functions as F

scoped_courses = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "level",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
        F.count(F.when(F.col("term") != "2024T1", 1)).alias("enrollment_term_count"),
    )
)

profiled = (
    scoped_courses.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count_2"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

profiled_2 = (
    profiled.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        "e.first_enrollment_enrolled_at",
    )
)

enrollment_stats_3 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count_3"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_2"),
        F.count("*").alias("enrollment_count_4"),
    )
)

profiled_3 = (
    profiled_2.alias("s")
    .join(enrollment_stats_3.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count_3"), F.lit(0)).alias("enrollment_count_3"),
        "e.last_enrollment_enrolled_at_2",
        F.coalesce(F.col("e.enrollment_count_4"), F.lit(0)).alias("enrollment_count_4"),
    )
)

annotated = (
    profiled_3
    .select(
        "*",
        F.substring(F.trim(F.col("level")), 1, 2).alias("level_prefix"),
        F.abs(F.col("enrollment_count_2") - 3).alias("enrollment_count_2_distance"),
    )
)

enrollment_stats_4 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count_5"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
    )
)

with_counts = (
    annotated.alias("s")
    .join(enrollment_stats_4.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count_5"), F.lit(0)).alias("enrollment_count_5"),
        "e.first_enrollment_enrolled_at_2",
    )
)

derived = (
    with_counts
    .select(
        "id",
        "course_code",
        "dept",
        "level",
        "last_enrollment_enrolled_at",
        "enrollment_term_count",
        "enrollment_count_2",
        "first_enrollment_enrolled_at",
        "enrollment_count_3",
        "last_enrollment_enrolled_at_2",
        "enrollment_count_4",
        "enrollment_count_5",
        F.length(F.trim(F.col("level"))).alias("level_len"),
        (F.col("enrollment_count_4") - F.col("enrollment_term_count")).alias("enrollment_count_4_minus_enrollment_term_count"),
        F.coalesce(F.col("level"), F.lit("UNKNOWN")).alias("level_or_unknown"),
    )
)

result = (
    derived
    .groupBy("level_len")
    .pivot("dept", ["Art", "Bio", "CS", "Biz"])
    .agg(F.max("enrollment_count_3"))
    .orderBy("level_len")
)
