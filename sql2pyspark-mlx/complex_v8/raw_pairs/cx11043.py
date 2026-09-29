from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    prepared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        "e.last_enrollment_enrolled_at",
    )
)

joined_course = (
    summarized_rows.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.score_filled",
        "s.enrollment_count",
        "s.last_enrollment_enrolled_at",
        F.col("c.level").alias("course_level"),
    )
)

course_level_stats = (
    joined_course
    .groupBy("course_level")
    .agg(F.avg("enrollment_count").alias("avg_enrollment_count"))
)

above_average = (
    joined_course.alias("s")
    .join(course_level_stats.alias("g"), F.col("g.course_level") == F.col("s.course_level"), "inner")
    .filter(F.col("s.enrollment_count") > F.col("g.avg_enrollment_count"))
    .select(
        "s.*",
        "g.avg_enrollment_count",
        F.round(F.col("s.enrollment_count") - F.col("g.avg_enrollment_count"), 2).alias("enrollment_count_minus_avg"),
    )
)

result = (
    above_average
    .filter(F.col("course_id") >= 5)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "score_filled",
        "enrollment_count",
        "last_enrollment_enrolled_at",
        "course_level",
        "avg_enrollment_count",
        "enrollment_count_minus_avg",
    )
    .orderBy(F.col("enrollment_count").desc_nulls_first())
)
