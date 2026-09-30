from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_score_bucket_distance = Window.orderBy("score_bucket_distance_per_score")
w_all_by_score_bucket_distance_2 = Window.orderBy("score_bucket_distance_per_score", "id")

filtered = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "score",
        "score_bucket",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "score",
        "days_since_enrolled_at",
        F.abs(F.col("score_bucket") - 30).alias("score_bucket_distance"),
        F.abs(F.col("days_since_enrolled_at") - 350).alias("days_since_enrolled_at_distance"),
        F.abs(F.col("score") - 32.7).alias("score_distance"),
    )
)

labeled = (
    scored
    .filter(F.col("score_bucket_distance") >= 35)
    .select(
        "id",
        "score",
        "score_bucket_distance",
        "score_distance",
        F.round(F.col("score_bucket_distance") / F.nullif(F.col("score"), F.lit(0)), 3).alias("score_bucket_distance_per_score"),
        F.greatest(F.col("score"), F.col("score_distance")).alias("greatest_score_score_distance"),
    )
)

tagged = (
    labeled
    .select(
        "id",
        "score",
        "score_bucket_distance",
        "score_distance",
        "score_bucket_distance_per_score",
        "greatest_score_score_distance",
        (F.floor(F.col("score_distance") / 25) * 25).alias("score_distance_bucket"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "score_bucket_distance",
        "score_bucket_distance_per_score",
        "greatest_score_score_distance",
        "score_distance_bucket",
        F.round(F.cume_dist().over(w_all_by_score_bucket_distance), 3).alias("score_bucket_distance_per_score_cume_dist"),
        F.round(F.percent_rank().over(w_all_by_score_bucket_distance_2), 3).alias("score_bucket_distance_per_score_percent_rank"),
    )
)

result = (
    ranked
    .select(
        "id",
        "score_bucket_distance",
        "score_bucket_distance_per_score",
        "score_bucket_distance_per_score_cume_dist",
        (F.floor(F.col("score_bucket_distance") / 25) * 25).alias("score_bucket_distance_bucket"),
        (F.floor(F.col("score_bucket_distance") / 25) * 25).alias("score_bucket_distance_bucket_2"),
    )
    .orderBy("score_bucket_distance_per_score")
)
