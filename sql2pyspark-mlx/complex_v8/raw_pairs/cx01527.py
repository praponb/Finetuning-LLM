from pyspark.sql import functions as F

matched = (
    df_enrollments.alias("a")
    .join(
        df_enrollments.alias("b"),
        (F.col("b.student_id") == F.col("a.student_id"))
        & (F.col("b.enrolled_at") < F.col("a.enrolled_at"))
        & (F.col("b.enrolled_at") >= F.col("a.enrolled_at") - F.expr("INTERVAL 7 DAYS")),
        "left",
    )
    .groupBy("a.id", "a.student_id", "a.enrolled_at")
    .agg(
        F.count("b.id").alias("prior_rows_7d"),
        F.sum("b.score").alias("prior_score_7d"),
    )
)

student_id_stats = (
    matched
    .groupBy("student_id")
    .agg(F.max("prior_rows_7d").alias("max_prior_rows_7d"))
)

outliers = (
    matched.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_rows_7d") == F.col("g.max_prior_rows_7d"))
    .select(
        "s.*",
        "g.max_prior_rows_7d",
        F.round(F.col("s.prior_rows_7d") - F.col("g.max_prior_rows_7d"), 2).alias("prior_rows_7d_minus_max"),
    )
)

annotated = (
    outliers
    .filter(F.col("prior_rows_7d_minus_max") >= -5.6)
    .select(
        "id",
        "enrolled_at",
        "prior_rows_7d",
        "prior_score_7d",
        "prior_rows_7d_minus_max",
        (F.col("prior_score_7d") + F.col("max_prior_rows_7d")).alias("prior_score_7d_plus_max_prior_rows_7d"),
        F.round(F.col("max_prior_rows_7d") * 1.1, 2).alias("max_prior_rows_7d_adj"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

result = (
    annotated
    .select(
        "id",
        "prior_rows_7d_minus_max",
        "prior_score_7d_plus_max_prior_rows_7d",
        "prior_score_7d",
    )
    .unpivot(
        ids=["id"],
        values=["prior_rows_7d_minus_max", "prior_score_7d_plus_max_prior_rows_7d", "prior_score_7d"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
