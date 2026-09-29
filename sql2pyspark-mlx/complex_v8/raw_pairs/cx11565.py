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

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.min("score").alias("min_enrollment_score"),
    )
)

with_counts = (
    covered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.min_enrollment_score")
)

result = (
    with_counts
    .filter(F.to_date(F.col("enrolled_at")) < F.lit("2025-02-18").cast("date"))
    .select(
        "id",
        "prior_rows_3d",
        "last_enrollment_enrolled_at",
        "min_enrollment_score",
        F.round(F.col("min_enrollment_score") * 1.5, 2).alias("min_enrollment_score_adj"),
    )
    .orderBy(F.col("min_enrollment_score").desc())
)
