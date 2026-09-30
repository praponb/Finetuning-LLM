from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("student_id") >= 12) & (F.col("term").isin("2024T2", "2024T1", "2025T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

filtered = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-08 08:00:00").cast("timestamp")) & (F.col("e.grade_status").isNotNull()), "left_anti")
)

line_level = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.term_grade_status_label",
        "s.grade_status_ref",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.term").alias("enrollment_term"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
    )
)

enrollment_grade_status_stats = (
    line_level
    .groupBy("enrollment_grade_status")
    .agg(F.percentile_approx("score", 0.5).alias("median_score"))
)

compared = (
    line_level.alias("s")
    .join(enrollment_grade_status_stats.alias("g"), F.col("g.enrollment_grade_status") == F.col("s.enrollment_grade_status"), "inner")
    .filter(F.col("s.score") >= F.col("g.median_score"))
    .select(
        "s.*",
        "g.median_score",
        F.round(F.col("s.score") - F.col("g.median_score"), 2).alias("score_minus_median"),
    )
)

stats = (
    compared
    .filter(F.col("score") <= 63.9)
    .groupBy(
        "student_id",
        F.to_date("enrollment_enrolled_at").alias("enrollment_enrolled_at_day"),
    )
    .agg(
        F.array_sort(F.collect_set("enrollment_term")).alias("enrollment_term_list"),
        F.round(F.sum("median_score") / F.nullif(F.sum("score_minus_median"), F.lit(0)), 3).alias("median_score_to_score_minus_median_ratio"),
        F.max("enrollment_enrolled_at").alias("last_enrollment_enrolled_at"),
        F.percentile_approx("median_score", 0.5).alias("median_median_score"),
        F.round(F.avg("score"), 2).alias("avg_score"),
        F.avg("score_minus_median").alias("avg_score_minus_median_check"),
    )
    .filter(F.col("avg_score_minus_median_check") > -30.0)
    .drop("avg_score_minus_median_check")
)

result = (
    stats
    .select(
        "student_id",
        "enrollment_enrolled_at_day",
        F.col("avg_score").alias("m_avg_score"),
        F.col("median_score_to_score_minus_median_ratio").alias("median_score_to_score_minus_median_ratio_v"),
    )
    .unpivot(
        ids=["student_id", "enrollment_enrolled_at_day"],
        values=["m_avg_score", "median_score_to_score_minus_median_ratio_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("student_id", "enrollment_enrolled_at_day", "field")
)
