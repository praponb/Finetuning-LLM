from pyspark.sql import functions as F

in_window = (
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

joined_student = (
    in_window.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_score_1d",
        F.col("st.gpa").alias("student_gpa"),
    )
)

result = (
    joined_student
    .filter(F.hour(F.col("enrolled_at")).between(7, 10))
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "student_gpa",
        F.date_format(F.col("enrolled_at"), "yyyy-MM").alias("enrolled_at_label"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
    .orderBy(F.col("student_id").asc_nulls_last(), "id")
    .limit(20)
)
