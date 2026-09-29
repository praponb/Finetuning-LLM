from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("score") >= 11.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 54.2).alias("score_distance"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

enriched = (
    prepared.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.enrolled_on") < F.lit("2025-03-20").cast("date"))
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term") == "2025T2", 1)).alias("enrollment_term_count"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

annotated = (
    with_counts
    .filter(F.col("score_distance") < 80.0)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "score_distance",
        "term_or_unknown",
        "student_enrolled_on",
        "student_major",
        "student_cohort",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("student_enrolled_on")).alias("days_since_student_enrolled_on"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

result = (
    annotated
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_or_unknown",
        "student_enrolled_on",
        "student_cohort",
        "days_since_student_enrolled_on",
        "term_ref",
        "grade_status_or_unknown",
        F.substring(F.trim(F.col("grade_status_or_unknown")), 1, 2).alias("grade_status_or_unknown_prefix"),
    )
    .orderBy("student_cohort")
)
