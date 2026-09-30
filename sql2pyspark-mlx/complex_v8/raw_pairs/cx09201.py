from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((~F.col("term").isin("2025T1", "2024T2")) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
    )
)

profiled = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

without_matches = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
)

enriched = (
    without_matches
    .select(
        "*",
        F.least(F.col("days_since_enrolled_at"), F.col("score")).alias("least_days_since_enrolled_at_score"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        (F.col("score") - F.col("enrollment_count_2")).alias("score_minus_enrollment_count_2"),
    )
)

combined_details = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.term_clean",
        "s.enrollment_count_2",
        "s.least_days_since_enrolled_at_score",
        "s.term_or_unknown",
        "s.score_minus_enrollment_count_2",
        "c.course_code",
        F.col("c.credits").alias("course_credits"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
    )
)

result = (
    combined_details
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_clean",
        "enrollment_count_2",
        "score_minus_enrollment_count_2",
        "course_credits",
        "student_enrolled_on",
        "student_name",
        F.when(F.col("term").isin("2025T2", "2024T2"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        (F.col("course_credits") + F.col("days_since_enrolled_at")).alias("course_credits_plus_days_since_enrolled_at"),
    )
    .orderBy("course_credits_plus_days_since_enrolled_at")
)
