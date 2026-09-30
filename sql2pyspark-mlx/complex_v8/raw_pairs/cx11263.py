from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("term") == "2025T1")
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

grade_status_stats = (
    valid_enrollments
    .groupBy("grade_status")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    valid_enrollments.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

joined = (
    vs_group.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.gpa") >= 2.7)
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.avg_score",
        "s.score_minus_avg",
        F.col("st.gpa").alias("student_gpa"),
    )
)

without_matches = (
    joined
    .crossJoin(joined.agg(F.avg("score_minus_avg").alias("overall_avg_score_minus_avg")))
    .filter(F.col("score_minus_avg") > F.col("overall_avg_score_minus_avg"))
    .drop("overall_avg_score_minus_avg")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

summarized_rows = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enriched = (
    summarized_rows
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        "days_since_enrolled_at",
        "score_minus_avg",
        "student_gpa",
        F.greatest(F.col("days_since_enrolled_at"), F.col("enrollment_count")).alias("greatest_days_since_enrolled_at_enrollment_count"),
    )
)

exploded_detail_enrollments = (
    enriched.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-19 15:00:00").cast("timestamp")), "inner")
    .select(
        "s.id",
        "s.course_id",
        "s.score",
        "s.enrolled_at",
        "s.days_since_enrolled_at",
        "s.student_gpa",
        "s.greatest_days_since_enrolled_at_enrollment_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
    )
)

result = (
    exploded_detail_enrollments
    .select(
        "id",
        "course_id",
        "score",
        "enrolled_at",
        "days_since_enrolled_at",
        "student_gpa",
        "greatest_days_since_enrolled_at_enrollment_count",
        "enrollment_student_id",
        "enrollment_score",
        F.round(F.col("student_gpa") / F.nullif(F.col("score"), F.lit(0)), 3).alias("student_gpa_per_score"),
        F.substring(F.trim(F.col("enrollment_grade_status")), 1, 2).alias("enrollment_grade_status_prefix"),
    )
    .orderBy(F.col("score").desc())
)
