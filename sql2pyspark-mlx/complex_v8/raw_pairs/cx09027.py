from pyspark.sql import functions as F

matched = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 3 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_3d"),
        F.sum("b.score").alias("prior_score_3d"),
    )
)

with_parent_student = (
    matched.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.cohort") != "C2021")
    .select(
        "s.*",
        F.col("st.major").alias("student_major"),
        F.col("st.cohort").alias("student_cohort"),
        F.col("st.gpa").alias("student_gpa"),
    )
)

without_matches = (
    with_parent_student
    .crossJoin(with_parent_student.agg(F.max("prior_score_3d").alias("overall_max_prior_score_3d")))
    .filter(F.col("prior_score_3d") < F.col("overall_max_prior_score_3d"))
    .drop("overall_max_prior_score_3d")
)

with_details = (
    without_matches.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.lower(F.col("st.student_name")).like("% f%")), "left")
    .select(
        "s.*",
        F.col("st.is_international").alias("student_is_international"),
        "st.student_name",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    with_details
    .select(
        "id",
        "enrolled_at",
        "prior_rows_3d",
        "student_major",
        "student_cohort",
        "student_is_international",
        "student_name",
        "student_enrolled_on",
        F.greatest(F.col("prior_rows_3d"), F.col("prior_score_3d")).alias("greatest_prior_rows_3d_prior_score_3d"),
        F.round(F.col("student_gpa") / F.nullif(F.col("prior_score_3d"), F.lit(0)), 3).alias("student_gpa_per_prior_score_3d"),
    )
    .orderBy("id", "enrolled_at")
)
