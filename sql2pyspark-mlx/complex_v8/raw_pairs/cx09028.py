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

annotated = (
    in_window
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_1d",
        F.abs(F.col("prior_score_1d") - 150.0).alias("prior_score_1d_distance"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
        F.when(F.col("prior_score_1d") >= 300.0, "large").when(F.col("prior_score_1d") >= 150.0, "medium").otherwise("small").alias("prior_score_1d_tier"),
    )
)

joined = (
    annotated.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.major") == "Math")
    .select("s.*", F.col("st.major").alias("student_major"))
)

result = (
    joined
    .filter(F.col("enrolled_at_hour") <= 14)
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_1d",
        "prior_score_1d_distance",
        "enrolled_at_hour",
        "prior_score_1d_tier",
        "student_major",
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
    .orderBy(
        F.col("prior_score_1d_distance").asc_nulls_last(),
        F.col("id").desc_nulls_first(),
    )
)
