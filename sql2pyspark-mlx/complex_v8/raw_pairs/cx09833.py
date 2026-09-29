from pyspark.sql import functions as F

cleaned = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score_filled",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled_2"),
        F.greatest(F.col("score"), F.col("score_filled")).alias("greatest_score_score_filled"),
    )
)
