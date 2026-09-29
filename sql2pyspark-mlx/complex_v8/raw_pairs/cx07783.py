from pyspark.sql import functions as F

joined_ranges = (
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

enriched = (
    joined_ranges
    .filter(F.col("enrolled_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .withColumn("prior_score_12h_adj", F.round(F.col("prior_score_12h") * 0.9, 2))
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count(F.when(~F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
    )
)

profiled = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    profiled
    .rollup("student_id", "enrollment_grade_status_count")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.percentile_approx("max_enrollment_score", 0.5).alias("median_max_enrollment_score"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("enrollment_grade_status_count").asc_nulls_last(),
    )
)
