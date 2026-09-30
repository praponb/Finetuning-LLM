from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.col("enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.abs(F.col("score") - 57.6).alias("score_distance"),
    )
)

with_details = (
    prepared_enrollments.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.grade_status",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

with_parent_student = (
    with_details.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.student_enrolled_on",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.cohort").alias("student_cohort"),
        "st.student_name",
    )
)

result = (
    with_parent_student
    .filter(F.col("student_name").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "student_enrolled_on",
        "student_is_international",
        "student_cohort",
        "student_name",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
        F.date_trunc("quarter", F.col("student_enrolled_on")).alias("student_enrolled_on_quarter"),
    )
    .orderBy(F.col("term").asc_nulls_last())
)
