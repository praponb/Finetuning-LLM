from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("score") >= 43.5) & (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.when(F.col("term").isin("2024T1", "2025T2"), "group_a").when(F.col("term") == "2024T2", "group_b").otherwise("other").alias("term_group"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

grouped = (
    base
    .groupBy("grade_status_or_unknown", "student_id")
    .agg(
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", F.col("score"))).alias("score_grade_status_incomplete"),
        F.percentile_approx("score", 0.5).alias("median_score"),
    )
)

qualified = (
    grouped.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") != "FAIL"), "left_semi")
)

result = (
    qualified
    .select("grade_status_or_unknown", "median_score")
    .orderBy("grade_status_or_unknown", F.col("median_score").desc_nulls_first())
)
