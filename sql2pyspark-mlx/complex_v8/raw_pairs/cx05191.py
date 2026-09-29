from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((~F.col("grade_status").isin("FAIL", "PASS")) & (F.col("score") > 8.7))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.substring(F.trim(F.col("term")), 1, 4).alias("term_prefix"),
    )
)

derived = (
    filtered
    .filter(F.col("score") >= 36.6)
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "term_grade_status_label",
        "term_prefix",
        F.abs(F.col("score") - 63.9).alias("score_distance"),
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

joined = (
    derived.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(~F.col("st.is_international"))
    .select(
        "s.*",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.is_international").alias("student_is_international"),
    )
)

exploded_detail_enrollments = (
    joined.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.score_adj",
        "s.student_enrolled_on",
        "s.student_cohort",
        "s.student_is_international",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

result = (
    exploded_detail_enrollments
    .filter(F.col("student_is_international"))
    .select(
        "id",
        "student_id",
        "student_cohort",
        "student_is_international",
        "enrollment_id",
        "enrollment_grade_status",
        "enrollment_term",
    )
    .orderBy(F.col("enrollment_grade_status").asc(), "enrollment_id")
)
