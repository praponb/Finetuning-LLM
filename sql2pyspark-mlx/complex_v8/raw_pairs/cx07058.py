from pyspark.sql import functions as F

joined_ranges = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 1 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_1d"),
        F.sum("b.score").alias("prior_score_1d"),
    )
)

enriched_student = (
    joined_ranges.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_1d",
        F.col("st.gpa").alias("student_gpa"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        "st.student_name",
    )
)

combined_details_student = (
    enriched_student.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.student_gpa",
        "s.student_enrolled_on",
        "s.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

result = (
    combined_details_student
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "student_gpa",
        "student_name",
        "student_is_international",
        F.coalesce(F.col("student_gpa"), F.lit(0.0)).alias("student_gpa_filled"),
        F.date_format(F.col("student_enrolled_on"), "yyyy-MM-dd").alias("student_enrolled_on_label"),
    )
    .orderBy(F.col("student_gpa_filled").desc(), F.col("student_id").desc())
)
