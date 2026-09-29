from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-18 08:00:00").cast("timestamp")) & (F.col("score") <= 78.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

enriched = (
    base_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.credits").alias("course_credits"), "c.course_code")
)

combined_details = (
    enriched.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.days_since_enrolled_at",
        "s.term_slug",
        "s.enrolled_at_plus_30m",
        "s.course_code",
        F.col("c.credits").alias("course_credits_2"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

result = (
    combined_details
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "term_slug",
        "enrolled_at_plus_30m",
        "course_credits_2",
        F.date_format(F.col("enrolled_at_plus_30m"), "yyyy-MM-dd HH:00").alias("enrolled_at_plus_30m_label"),
        F.quarter(F.col("enrolled_at_plus_30m")).alias("enrolled_at_plus_30m_quarter"),
    )
    .orderBy(F.col("score").desc_nulls_first())
)
