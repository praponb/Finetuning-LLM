from pyspark.sql import functions as F

base = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.trim(F.col("term"))).alias("term_clean"),
    )
)

derived = (
    base
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "term_clean",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

labeled = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "term_clean",
        "grade_status_or_unknown",
        "grade_status_ref",
        "grade_status_clean",
        F.substring(F.trim(F.col("grade_status_ref")), 1, 3).alias("grade_status_ref_prefix"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
        F.lower(F.regexp_replace(F.trim(F.col("grade_status_clean")), " ", "_")).alias("grade_status_clean_slug"),
    )
)

exploded_detail = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_or_unknown",
        "s.grade_status_ref_prefix",
        "s.grade_status_clean_slug",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

exploded_detail_2 = (
    exploded_detail.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.grade_status_or_unknown",
        "s.grade_status_ref_prefix",
        "s.enrollment_id",
        "s.enrollment_term",
        "s.enrollment_enrolled_at",
        F.col("e.id").alias("enrollment_id_2"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at_2"),
    )
)

summary = (
    exploded_detail_2
    .groupBy(
        F.date_trunc("month", "enrollment_enrolled_at_2").alias("enrollment_enrolled_at_2_month"),
    )
    .agg(
        F.count(F.when(~F.col("enrollment_term").isin("2024T1", "2024T2"), 1)).alias("n_enrollment_term_not_in_2024t"),
        F.count("*").alias("n_rows"),
        F.min("enrollment_enrolled_at_2").alias("first_enrollment_enrolled_at_2"),
        F.round(100 * F.count(F.when(~F.col("enrollment_term").isin("2024T1", "2025T1"), 1)) / F.count("*"), 1).alias("pct_enrollment_term_not_in_2024t"),
        F.max("enrollment_enrolled_at_2").alias("last_enrollment_enrolled_at_2"),
    )
    .filter(F.col("n_rows") >= 3)
)

without_matches = (
    summary
    .crossJoin(summary.agg(F.min("n_rows").alias("overall_min_n_rows")))
    .filter(F.col("n_rows") > F.col("overall_min_n_rows"))
    .drop("overall_min_n_rows")
)

result = (
    without_matches
    .filter(F.col("enrollment_enrolled_at_2_month") >= F.lit("2025-01-13 09:00:00").cast("timestamp"))
    .select(
        "enrollment_enrolled_at_2_month",
        "n_enrollment_term_not_in_2024t",
        "first_enrollment_enrolled_at_2",
        "last_enrollment_enrolled_at_2",
        F.when(F.col("n_rows") >= 10, "A").when(F.col("n_rows") >= 4, "B").otherwise("C").alias("n_rows_tier"),
    )
    .orderBy(F.col("n_rows_tier").desc(), "enrollment_enrolled_at_2_month")
    .limit(20)
)
