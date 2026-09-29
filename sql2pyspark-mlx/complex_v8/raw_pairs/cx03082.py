from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_children_enrollments = (
    filtered_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.term").alias("enrollment_term"),
    )
)

tagged = (
    with_children_enrollments
    .filter(F.col("score") > 38.3)
    .select(
        "*",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.upper(F.trim(F.col("enrollment_term"))).alias("enrollment_term_clean"),
        F.round(F.col("enrollment_score") * 0.9, 2).alias("enrollment_score_adj"),
    )
)

result = (
    tagged
    .select(
        "id",
        "enrollment_id",
        "score",
        "enrollment_score",
        "enrollment_score_adj",
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["score", "enrollment_score", "enrollment_score_adj"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "enrollment_id", "measure")
)
