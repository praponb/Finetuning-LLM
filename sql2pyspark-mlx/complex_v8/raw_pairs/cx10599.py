from pyspark.sql import functions as F

joined_ranges = (
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
    joined_ranges.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-02-24").cast("date")) & (F.col("e.grade_status").isNotNull()), "left_anti")
)

result = (
    qualified
    .select(
        "id",
        "enrolled_at",
        "prior_rows_3d",
        "prior_score_3d",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.abs(F.col("prior_rows_3d") - 4).alias("prior_rows_3d_distance"),
    )
    .orderBy("days_since_enrolled_at", F.col("prior_rows_3d").asc_nulls_last())
)
