from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") & (F.col("enrolled_at") >= F.lit("2025-02-12 09:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

qualified = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.course_id") >= 10), "left_anti")
    .filter(F.col("term").isNotNull())
)

screened = (
    qualified.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score").between(20.6, 94.2)) & (F.col("e.grade_status") == "FAIL"), "left_semi")
)

result = (
    screened
    .groupingSets(
        [["term", "student_id"], ["term"], ["student_id"]],
        "term", "student_id",
    )
    .agg(
        F.count("*").alias("n_rows"),
        F.round(F.sum("score") / F.nullif(F.sum("score_bucket"), F.lit(0)), 3).alias("score_to_score_bucket_ratio"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
