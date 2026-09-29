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

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    in_window.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

student_id_stats = (
    profiled
    .groupBy("student_id")
    .agg(F.percentile_approx("prior_score_3d", 0.5).alias("median_prior_score_3d"))
)

vs_group = (
    profiled.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_score_3d") >= F.col("g.median_prior_score_3d"))
    .select(
        "s.*",
        "g.median_prior_score_3d",
        F.round(F.col("s.prior_score_3d") - F.col("g.median_prior_score_3d"), 2).alias("prior_score_3d_minus_median"),
    )
)

result = (
    vs_group
    .select(
        "id",
        "enrolled_at",
        "prior_rows_3d",
        "prior_score_3d",
        "first_enrollment_enrolled_at",
        "last_enrollment_enrolled_at",
        "enrollment_count",
        "median_prior_score_3d",
        "prior_score_3d_minus_median",
        F.abs(F.col("prior_rows_3d") - 3).alias("prior_rows_3d_distance"),
        F.greatest(F.col("median_prior_score_3d"), F.col("enrollment_count")).alias("greatest_median_prior_score_3d_enrollment_count"),
    )
    .orderBy(F.col("prior_rows_3d_distance").desc_nulls_first())
)
