from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("enrolled_at") >= F.lit("2025-02-06 10:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

qualified = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") != "2025T1") & (F.col("e.student_id") >= 5), "left_anti")
    .filter(F.col("course_id").isNotNull())
)

result = (
    qualified
    .select("id", "score", "score_adj")
    .unpivot(
        ids=["id"],
        values=["score", "score_adj"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
