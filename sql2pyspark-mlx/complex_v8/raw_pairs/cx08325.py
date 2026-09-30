from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .filter(F.col("term").isin("2025T2", "2025T1", "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

derived = (
    cleaned
    .select(
        "*",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("course_id").cast("string")).alias("grade_status_ref"),
    )
)

result = (
    derived
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "score_bucket",
        "score_filled",
        "grade_status_ref",
        (F.col("score_filled") + F.col("score")).alias("score_filled_plus_score"),
        F.round(F.col("score") / F.nullif(F.col("score_bucket"), F.lit(0)), 3).alias("score_per_score_bucket"),
    )
    .orderBy(F.col("score_filled").desc_nulls_first(), "id")
    .limit(20)
)
