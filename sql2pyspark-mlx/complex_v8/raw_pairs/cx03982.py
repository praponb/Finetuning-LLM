from pyspark.sql import functions as F

covered = (
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

derived = (
    covered
    .select(
        "*",
        F.least(F.col("prior_rows_1d"), F.col("prior_score_1d")).alias("least_prior_rows_1d_prior_score_1d"),
        F.when(F.col("prior_score_1d") >= 400.0, "large").when(F.col("prior_score_1d") >= 225.0, "medium").otherwise("small").alias("prior_score_1d_tier"),
        F.round(F.col("prior_score_1d") * 1.5, 2).alias("prior_score_1d_adj"),
    )
)

result = (
    derived
    .filter(F.col("prior_score_1d_adj") >= 262.5)
    .select(
        "id",
        "prior_rows_1d",
        "prior_score_1d",
        "least_prior_rows_1d_prior_score_1d",
        "prior_score_1d_tier",
        "prior_score_1d_adj",
        F.least(F.col("prior_score_1d_adj"), F.col("prior_rows_1d")).alias("least_prior_score_1d_adj_prior_rows_1d"),
        F.coalesce(F.col("prior_score_1d_tier"), F.lit("UNKNOWN")).alias("prior_score_1d_tier_or_unknown"),
    )
    .orderBy(
        "least_prior_rows_1d_prior_score_1d",
        F.col("prior_score_1d_tier").asc(),
    )
)
