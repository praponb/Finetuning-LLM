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

per_group = (
    covered
    .groupBy(
        "prior_rows_3d",
        F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"),
        "student_id",
    )
    .agg(
        F.round(F.avg("prior_rows_3d"), 2).alias("avg_prior_rows_3d"),
        F.count("*").alias("n_rows"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_counts = (
    per_group.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.avg_enrollment_score", "e.last_enrollment_enrolled_at")
)

result = (
    with_counts
    .select(
        "prior_rows_3d",
        "enrolled_at_ym",
        "avg_enrollment_score",
        "avg_prior_rows_3d",
    )
    .unpivot(
        ids=["prior_rows_3d", "enrolled_at_ym"],
        values=["avg_enrollment_score", "avg_prior_rows_3d"],
        variableColumnName="field",
        valueColumnName="reading",
    )
)
