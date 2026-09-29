from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-03-02").cast("date")) & (F.col("grade_status").isin("PASS", "FAIL")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

scored = (
    prepared
    .filter(F.col("score").between(10.1, 77.0))
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "grade_status",
        "grade_status_or_unknown",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.length(F.trim(F.col("term"))).alias("term_len_2"),
        F.round(F.col("score") / F.nullif(F.col("score_adj"), F.lit(0)), 3).alias("score_per_score_adj"),
    )
)

combined_details = (
    scored.alias("s")
    .join(df_students.alias("st"), F.col("st.id") == F.col("s.student_id"), "inner")
    .filter(F.col("st.enrolled_on").between(F.lit("2024-02-28").cast("date"), F.lit("2025-07-03").cast("date")))
    .select(
        "s.id",
        "s.student_id",
        "s.term",
        "s.score",
        "s.grade_status",
        "s.score_bucket",
        "s.score_per_score_adj",
        F.col("st.enrolled_on").alias("student_enrolled_on"),
    )
)

qualified = (
    combined_details
    .crossJoin(combined_details.agg(F.max("score").alias("overall_max_score")))
    .filter(F.col("score") < F.col("overall_max_score"))
    .drop("overall_max_score")
)

result = (
    qualified
    .select("id", "score", "score_per_score_adj")
    .unpivot(
        ids=["id"],
        values=["score", "score_per_score_adj"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
