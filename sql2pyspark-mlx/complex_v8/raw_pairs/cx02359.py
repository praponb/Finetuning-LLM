from pyspark.sql import functions as F

covered = (
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

combined_details_student = (
    covered.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

combined_details_student_2 = (
    combined_details_student.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.cohort").isin("C2023", "C2021")), "inner")
    .filter(F.col("st.enrolled_on") < F.lit("2025-07-18").cast("date"))
    .select(
        "s.id",
        "s.student_id",
        "s.prior_score_12h",
        "s.student_enrolled_on",
        "s.student_gpa",
        F.col("st.enrolled_on").alias("student_enrolled_on_2"),
        F.col("st.cohort").alias("student_cohort"),
    )
)

enriched = (
    combined_details_student_2
    .filter(F.col("prior_score_12h") >= 175.0)
    .select(
        "id",
        "prior_score_12h",
        "student_enrolled_on",
        "student_gpa",
        "student_enrolled_on_2",
        "student_cohort",
        F.coalesce(F.col("student_cohort"), F.lit("UNKNOWN")).alias("student_cohort_or_unknown"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("student_enrolled_on")).alias("days_since_student_enrolled_on"),
    )
)

result = (
    enriched
    .select(
        "id",
        "prior_score_12h",
        "student_enrolled_on",
        "student_gpa",
        "student_cohort_or_unknown",
        "days_since_student_enrolled_on",
        F.when(F.col("student_gpa") >= 3.3, "HIGH").when(F.col("student_gpa") >= 2.7, "MEDIUM").otherwise("LOW").alias("student_gpa_tier"),
    )
)
