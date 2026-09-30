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

student_id_stats = (
    in_window
    .groupBy("student_id")
    .agg(F.max("prior_rows_3d").alias("max_prior_rows_3d"))
)

compared = (
    in_window.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_rows_3d") == F.col("g.max_prior_rows_3d"))
    .select(
        "s.*",
        "g.max_prior_rows_3d",
        F.round(F.col("s.prior_rows_3d") - F.col("g.max_prior_rows_3d"), 2).alias("prior_rows_3d_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    compared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

without_matches = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("prior_rows_3d").alias("overall_min_prior_rows_3d")))
    .filter(F.col("prior_rows_3d") > F.col("overall_min_prior_rows_3d"))
    .drop("overall_min_prior_rows_3d")
)

result = (
    without_matches
    .select(
        "id",
        F.col("prior_rows_3d_minus_max").alias("prior_rows_3d_minus_max_v"),
        F.col("max_prior_rows_3d").alias("m_max_prior_rows_3d"),
        F.col("prior_score_3d").alias("m_prior_score_3d"),
    )
    .unpivot(
        ids=["id"],
        values=["prior_rows_3d_minus_max_v", "m_max_prior_rows_3d", "m_prior_score_3d"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "measure")
)
