from pyspark.sql import functions as F

covered = (
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

with_activity = (
    covered
    .crossJoin(covered.agg(F.max("prior_score_7d").alias("overall_max_prior_score_7d")))
    .filter(F.col("prior_score_7d") < F.col("overall_max_prior_score_7d"))
    .drop("overall_max_prior_score_7d")
)

student_id_stats = (
    with_activity
    .groupBy("student_id")
    .agg(F.avg("prior_score_7d").alias("avg_prior_score_7d"))
)

vs_group = (
    with_activity.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.prior_score_7d") > F.col("g.avg_prior_score_7d"))
    .select(
        "s.*",
        "g.avg_prior_score_7d",
        F.round(F.col("s.prior_score_7d") - F.col("g.avg_prior_score_7d"), 2).alias("prior_score_7d_minus_avg"),
    )
)

with_children = (
    vs_group.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.enrolled_at",
        "s.prior_score_7d",
        "s.avg_prior_score_7d",
        "s.prior_score_7d_minus_avg",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
    )
)

result = (
    with_children
    .groupingSets(
        [["enrollment_id", "enrollment_grade_status", "enrollment_course_id"], ["enrollment_id"], ["enrollment_grade_status"]],
        "enrollment_id", "enrollment_grade_status", "enrollment_course_id",
    )
    .agg(
        F.round(F.avg("avg_prior_score_7d"), 2).alias("avg_avg_prior_score_7d"),
        F.min("prior_score_7d_minus_avg").alias("min_prior_score_7d_minus_avg"),
        F.percentile_approx("prior_score_7d", 0.5).alias("median_prior_score_7d"),
    )
    .orderBy(
        F.col("enrollment_id").asc_nulls_last(),
        F.col("enrollment_grade_status").asc_nulls_last(),
        F.col("enrollment_course_id").asc_nulls_last(),
    )
)
