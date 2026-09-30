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

grouped = (
    in_window
    .groupBy("student_id")
    .agg(
        F.percentile_approx("prior_rows_1d", 0.5).alias("median_prior_rows_1d"),
        F.round(F.stddev("prior_rows_1d"), 2).alias("stddev_prior_rows_1d"),
    )
)

result = (
    grouped
    .select(
        "student_id",
        "median_prior_rows_1d",
        "stddev_prior_rows_1d",
        F.abs(F.col("median_prior_rows_1d") - 4).alias("median_prior_rows_1d_distance"),
        F.round(F.col("stddev_prior_rows_1d") / F.nullif(F.col("median_prior_rows_1d"), F.lit(0)), 3).alias("stddev_prior_rows_1d_per_median_prior_rows_1d"),
    )
)
