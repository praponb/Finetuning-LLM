from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter((F.col("student_id") >= 17) & (F.hour(F.col("enrolled_at")).between(6, 8)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

screened = (
    recent_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") > 11.1), "left_semi")
    .filter(F.col("score") <= 71.0)
)

derived = (
    screened
    .select(
        "*",
        F.when(F.col("grade_status_or_unknown").isin("UNKNOWN", "PASS"), "group_a").when(F.col("grade_status_or_unknown") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_or_unknown_group"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

result = (
    derived
    .select("id", "score_filled", "score")
    .unpivot(
        ids=["id"],
        values=["score_filled", "score"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
