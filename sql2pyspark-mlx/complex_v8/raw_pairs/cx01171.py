from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

with_counts = (
    filtered_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

scored = (
    with_counts
    .filter(F.col("avg_enrollment_score").between(15.0, 65.0))
    .select(
        "*",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        (F.floor(F.col("avg_enrollment_score") / 25) * 25).alias("avg_enrollment_score_bucket"),
    )
)

with_parent_student = (
    scored.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "s.term_or_unknown",
        "s.avg_enrollment_score",
        "s.enrolled_at_label",
        "s.avg_enrollment_score_bucket",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.is_international").alias("student_is_international"),
        "c.course_code",
    )
)

result = (
    with_parent_student
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "enrolled_at",
        "term_or_unknown",
        "avg_enrollment_score",
        "enrolled_at_label",
        "student_enrolled_on",
        "student_is_international",
        "course_code",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("student_enrolled_on")).alias("days_since_student_enrolled_on"),
    )
)
