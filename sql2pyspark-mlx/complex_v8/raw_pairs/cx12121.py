from pyspark.sql import functions as F

covered = (
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

scored = (
    covered
    .filter(F.col("prior_rows_3d") > 2)
    .select(
        "*",
        (F.col("prior_rows_3d") - F.col("prior_score_3d")).alias("prior_rows_3d_minus_prior_score_3d"),
        F.greatest(F.col("prior_score_3d"), F.col("prior_rows_3d")).alias("greatest_prior_score_3d_prior_rows_3d"),
    )
)

enriched_student = (
    scored.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_3d",
        "s.prior_rows_3d_minus_prior_score_3d",
        "s.greatest_prior_score_3d_prior_rows_3d",
        F.col("st.major").alias("student_major"),
        "st.student_name",
        F.col("st.is_international").alias("student_is_international"),
    )
)

enriched_student_2 = (
    enriched_student.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_3d",
        "s.prior_rows_3d_minus_prior_score_3d",
        "s.greatest_prior_score_3d_prior_rows_3d",
        "s.student_major",
        "s.student_name",
        "s.student_is_international",
        F.col("st.student_name").alias("student_student_name"),
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

result = (
    enriched_student_2
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_3d",
        "prior_rows_3d_minus_prior_score_3d",
        "greatest_prior_score_3d_prior_rows_3d",
        "student_is_international",
        "student_student_name",
        "student_enrolled_on",
        F.length(F.trim(F.col("student_major"))).alias("student_major_len"),
    )
    .orderBy("student_enrolled_on", F.col("student_student_name").asc())
)
