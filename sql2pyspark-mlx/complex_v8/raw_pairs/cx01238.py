from pyspark.sql import functions as F

in_window = (
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

qualified = (
    in_window.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") < 59.8), "left_anti")
    .filter(F.col("prior_rows_3d") <= 6)
)

combined_details_student = (
    qualified.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_rows_3d",
        "s.prior_score_3d",
        F.col("st.major").alias("student_major"),
    )
)

filtered = (
    combined_details_student.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-02 08:00:00").cast("timestamp")), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_3d",
        "prior_score_3d",
        "student_major",
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
    .orderBy(F.col("student_id").desc(), F.col("id").desc_nulls_first())
)
