from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("score") > 11.3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.year(F.col("enrolled_at")).alias("enrolled_at_year"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

screened = (
    recent
    .crossJoin(recent.agg(F.min("score").alias("overall_min_score")))
    .filter(F.col("score") > F.col("overall_min_score"))
    .drop("overall_min_score")
)

result = (
    screened
    .select("id", "days_since_enrolled_at", "enrolled_at_year")
    .unpivot(
        ids=["id"],
        values=["days_since_enrolled_at", "enrolled_at_year"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
