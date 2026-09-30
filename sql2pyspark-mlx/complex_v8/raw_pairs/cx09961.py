from pyspark.sql import functions as F

covered = (
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

scored = (
    covered
    .select(
        "*",
        F.least(F.col("prior_score_12h"), F.col("prior_rows_12h")).alias("least_prior_score_12h_prior_rows_12h"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
)

result = (
    scored
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_score_12h",
        "least_prior_score_12h_prior_rows_12h",
        (F.floor(F.col("prior_score_12h") / 100) * 100).alias("prior_score_12h_bucket"),
        F.round(F.col("prior_rows_12h") / F.nullif(F.col("prior_score_12h"), F.lit(0)), 3).alias("prior_rows_12h_per_prior_score_12h"),
    )
    .orderBy(F.col("least_prior_score_12h_prior_rows_12h").asc_nulls_last())
)
