from pyspark.sql import functions as F

matched = (
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

student_id_stats = (
    matched
    .groupBy("student_id")
    .agg(F.avg("prior_score_1d").alias("avg_prior_score_1d"))
)

above_average = (
    matched.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_score_1d") > F.col("g.avg_prior_score_1d"))
    .select(
        "s.*",
        "g.avg_prior_score_1d",
        F.round(F.col("s.prior_score_1d") - F.col("g.avg_prior_score_1d"), 2).alias("prior_score_1d_minus_avg"),
    )
)

result = (
    above_average
    .select(
        "id",
        "student_id",
        "enrolled_at",
        "prior_rows_1d",
        "avg_prior_score_1d",
        "prior_score_1d_minus_avg",
        F.round(F.col("prior_score_1d_minus_avg") * 0.9, 2).alias("prior_score_1d_minus_avg_adj"),
        F.date_trunc("week", F.col("enrolled_at")).alias("enrolled_at_week"),
    )
    .orderBy(F.col("student_id").desc())
)
