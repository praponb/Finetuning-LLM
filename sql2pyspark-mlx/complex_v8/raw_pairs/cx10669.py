from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("student_id") <= 19) & (F.col("score") <= 63.9))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.initcap(F.trim(F.col("term"))).alias("term_clean"),
    )
)

without_matches = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-31 09:00:00").cast("timestamp")) & (F.col("e.score").between(5.7, 80.2)), "left_semi")
    .filter(F.col("enrolled_at_date") < F.lit("2025-01-01").cast("date"))
)

result = (
    without_matches
    .groupingSets(
        [["term", "student_id", "grade_status_or_unknown"], ["term"], ["student_id"], []],
        "term", "student_id", "grade_status_or_unknown",
    )
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.count("*").alias("row_count"),
        F.round(F.avg("score"), 2).alias("avg_score"),
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("grade_status_or_unknown").asc_nulls_last(),
    )
)
