from pyspark.sql import functions as F

covered = (
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

with_activity = (
    covered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") <= 5) & (F.hour(F.col("e.enrolled_at")).between(8, 12)), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_1d",
        F.round(F.col("prior_rows_1d") / F.nullif(F.col("prior_score_1d"), F.lit(0)), 3).alias("prior_rows_1d_per_prior_score_1d"),
        F.when(F.col("prior_score_1d") >= 350.0, "HIGH").when(F.col("prior_score_1d") >= 100.0, "MEDIUM").otherwise("LOW").alias("prior_score_1d_tier"),
    )
    .orderBy("prior_rows_1d_per_prior_score_1d", "id")
    .limit(5)
)
