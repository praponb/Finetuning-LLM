from pyspark.sql import functions as F

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.score").alias("prior_score_12h"),
    )
)

line_level = (
    in_window.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

screened = (
    line_level.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term").isin("2024T2", "2024T1")) & (F.col("e.grade_status") == "PASS"), "left_semi")
)

enriched_student = (
    screened.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .join(df_enrollments.alias("e"), F.col("e.id") == F.col("s.enrollment_id"), "left")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        F.col("st.major").alias("student_major"),
        F.col("e.term").alias("enrollment_term_2"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    enriched_student
    .filter(F.col("enrollment_id") >= 7)
    .select(
        "id",
        "enrolled_at",
        "enrollment_id",
        "enrollment_course_id",
        "student_is_international",
        "student_major",
        "enrollment_grade_status",
    )
    .orderBy(F.col("id").asc())
)
