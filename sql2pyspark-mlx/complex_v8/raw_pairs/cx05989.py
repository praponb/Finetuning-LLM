from pyspark.sql import functions as F

in_window = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

derived = (
    in_window
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_7d",
        "prior_score_7d",
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
        F.when(F.col("prior_score_7d") >= 350.0, "A").when(F.col("prior_score_7d") >= 200.0, "B").otherwise("C").alias("prior_score_7d_tier"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

result = (
    derived
    .select(
        "id",
        "prior_rows_7d",
        "prior_score_7d",
        "prior_score_7d_tier",
    )
)
