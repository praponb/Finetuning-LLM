from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 36.6)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 1.07, 2).alias("score_adj"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.when(F.col("score") >= 78.8, "HIGH").when(F.col("score") >= 38.3, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

tagged = (
    valid_enrollments
    .select(
        "id",
        "score",
        "grade_status",
        "enrolled_at",
        "score_adj",
        "score_filled",
        "score_tier",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.substring(F.trim(F.col("score_tier")), 1, 3).alias("score_tier_prefix"),
        F.abs(F.col("score_adj") - 52.5).alias("score_adj_distance"),
    )
)

filtered = (
    tagged
    .crossJoin(tagged.agg(F.min("score_bucket").alias("overall_min_score_bucket")))
    .filter(F.col("score_bucket") > F.col("overall_min_score_bucket"))
    .drop("overall_min_score_bucket")
)

derived = (
    filtered
    .filter(F.col("score_filled") <= 90.0)
    .select(
        "id",
        "score",
        "enrolled_at",
        "score_filled",
        "score_tier",
        "score_tier_prefix",
        "score_adj_distance",
        F.greatest(F.col("score_bucket"), F.col("score_adj")).alias("greatest_score_bucket_score_adj"),
        F.coalesce(F.col("score_filled"), F.lit(0.0)).alias("score_filled_filled"),
    )
)

enriched = (
    derived
    .select(
        "id",
        "enrolled_at",
        "score_tier",
        "score_tier_prefix",
        "score_adj_distance",
        "score_filled_filled",
        F.round(F.col("score") / F.nullif(F.col("score_filled"), F.lit(0)), 3).alias("score_per_score_filled"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

result = (
    enriched
    .filter(F.col("score_tier_prefix").isNotNull())
    .select(
        "id",
        "score_tier",
        "score_adj_distance",
        "score_filled_filled",
        "score_per_score_filled",
        "enrolled_at_date",
        F.when(F.col("score_adj_distance") >= 97.5, "A").when(F.col("score_adj_distance") >= 45.0, "B").otherwise("C").alias("score_adj_distance_tier"),
        F.concat_ws("|", F.col("score_tier"), F.col("score_tier_prefix")).alias("score_tier_score_tier_prefix_label"),
    )
    .orderBy("score_adj_distance", F.col("score_tier").desc(), "id")
    .limit(5)
)
