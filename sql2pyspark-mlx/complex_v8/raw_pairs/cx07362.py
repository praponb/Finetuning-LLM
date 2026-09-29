from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

summary = (
    valid_enrollments
    .filter(F.col("grade_status") != "INCOMPLETE")
    .groupBy("student_id", "grade_status")
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("enrolled_at_year"), F.lit(0)), 3).alias("score_to_enrolled_at_year_ratio"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("term").alias("distinct_terms"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term") == "2025T1", 1)).alias("enrollment_term_count"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

summarized_rows = (
    summary.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        "e.first_enrollment_enrolled_at",
    )
)

qualified = (
    summarized_rows.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") < 63.9) & (F.col("e.enrolled_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")), "left_anti")
)

result = (
    qualified
    .select(
        "student_id",
        "grade_status",
        "stddev_score",
        "score_to_enrolled_at_year_ratio",
    )
    .unpivot(
        ids=["student_id", "grade_status"],
        values=["stddev_score", "score_to_enrolled_at_year_ratio"],
        variableColumnName="measure",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
