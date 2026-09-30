from pyspark.sql import functions as F

prepared_enrollments = (
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
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
    )
)

screened = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.term") != "2024T2"), "left_anti")
    .filter(F.col("score_adj") < 97.5)
)

grade_status_stats = (
    screened
    .groupBy("grade_status")
    .agg(F.max("score_adj").alias("max_score_adj"))
)

outliers = (
    screened.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.score_adj") == F.col("g.max_score_adj"))
    .select(
        "s.*",
        "g.max_score_adj",
        F.round(F.col("s.score_adj") - F.col("g.max_score_adj"), 2).alias("score_adj_minus_max"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

summarized_rows = (
    outliers.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

result = (
    summarized_rows
    .select(
        "id",
        F.col("max_enrollment_score").alias("m_max_enrollment_score"),
        F.col("score_adj").alias("m_score_adj"),
    )
    .unpivot(
        ids=["id"],
        values=["m_max_enrollment_score", "m_score_adj"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
