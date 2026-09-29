from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

without_matches = (
    recent_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa") < 3.4) & (F.col("st.major") != "Biz"), "left_semi")
)

tagged = (
    without_matches
    .filter(F.col("enrolled_at_date").isNotNull())
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        "enrolled_at_date",
        "days_since_enrolled_at",
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
        F.upper(F.trim(F.col("term"))).alias("term_clean_2"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.sum("score").alias("total_enrollment_score"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_stats = (
    tagged.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.total_enrollment_score",
        "e.max_enrollment_score",
    )
)

result = (
    with_stats
    .rollup("course_id", "term", "enrollment_count")
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
        F.col("enrollment_count").asc_nulls_last(),
    )
)
