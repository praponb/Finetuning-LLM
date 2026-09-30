from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") > 16.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

enriched = (
    filtered_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .filter(F.upper(F.trim(F.col("c.course_code"))).like("C%"))
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.term_len",
        "s.score_adj",
        "s.days_since_enrolled_at",
        "c.course_code",
    )
)

line_level_enrollments = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.enrolled_at",
        "s.score_adj",
        "s.days_since_enrolled_at",
        "s.course_code",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

detail_rows_enrollments = (
    line_level_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.course_code",
        "s.enrollment_id",
        "s.enrollment_student_id",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.student_id").alias("enrollment_student_id_2"),
        F.col("e.grade_status").alias("enrollment_grade_status_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    detail_rows_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.enrollment_student_id_2"), "left")
    .select("s.*", "e.max_enrollment_score", "e.last_enrollment_enrolled_at")
)

with_details = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.course_code",
        "s.enrollment_id",
        "s.enrollment_student_id",
        "s.enrollment_id_2",
        "s.enrollment_student_id_2",
        "s.enrollment_grade_status_2",
        "s.max_enrollment_score",
        "s.last_enrollment_enrolled_at",
        F.col("e.student_id").alias("enrollment_student_id_3"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_counts = (
    with_details.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    with_counts
    .filter(F.col("max_enrollment_score") >= 30.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "course_code",
        "enrollment_id",
        "enrollment_student_id",
        "enrollment_id_2",
        "enrollment_student_id_2",
        "max_enrollment_score",
        "course_credits",
        F.when(F.col("course_level").isin("core", "intro"), "group_a").when(F.col("course_level") == "advanced", "group_b").otherwise("other").alias("course_level_group"),
        F.greatest(F.col("course_credits"), F.col("max_enrollment_score")).alias("greatest_course_credits_max_enrollment_score"),
    )
)
