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
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

with_counts = (
    covered.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
    )
)

enriched = (
    with_counts
    .select(
        "*",
        (F.floor(F.col("min_enrollment_score") / 25) * 25).alias("min_enrollment_score_bucket"),
        F.least(F.col("prior_rows_3d"), F.col("prior_score_3d")).alias("least_prior_rows_3d_prior_score_3d"),
        F.round(F.col("prior_score_3d") * 1.5, 2).alias("prior_score_3d_adj"),
    )
)

result = (
    enriched
    .filter(F.col("prior_rows_3d") > 4)
    .select(
        "id",
        "prior_score_3d",
        "enrollment_count",
        "min_enrollment_score",
        "min_enrollment_score_bucket",
        "least_prior_rows_3d_prior_score_3d",
        "prior_score_3d_adj",
        F.round(F.col("prior_score_3d_adj") * 1.5, 2).alias("prior_score_3d_adj_adj"),
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
    )
)
