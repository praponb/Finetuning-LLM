from pyspark.sql import functions as F

joined_ranges = (
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

without_matches = (
    joined_ranges.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") != "2024T1"), "left_semi")
    .filter(F.col("enrolled_at") < F.lit("2025-02-24 12:00:00").cast("timestamp"))
)

result = (
    without_matches
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_12h",
        "prior_score_12h",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
    .orderBy(F.col("prior_rows_12h").desc(), F.col("enrolled_at").desc(), "id")
    .limit(5)
)
