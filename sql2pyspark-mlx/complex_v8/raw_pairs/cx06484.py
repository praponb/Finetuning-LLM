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

screened = (
    in_window
    .crossJoin(in_window.agg(F.max("prior_rows_1d").alias("overall_max_prior_rows_1d")))
    .filter(F.col("prior_rows_1d") < F.col("overall_max_prior_rows_1d"))
    .drop("overall_max_prior_rows_1d")
)

result = (
    screened
    .select(
        "id",
        "enrolled_at",
        "prior_rows_1d",
        "prior_score_1d",
        (F.floor(F.col("prior_score_1d") / 100) * 100).alias("prior_score_1d_bucket"),
    )
    .orderBy("prior_rows_1d")
)
