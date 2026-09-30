from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter(F.col("grade_status").isin("PASS", "INCOMPLETE", "FAIL"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 59.7).alias("score_distance"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

tagged = (
    prepared
    .select(
        "id",
        "student_id",
        "term",
        "grade_status",
        "enrolled_at",
        "score_distance",
        "score_bucket",
        (F.floor(F.col("score_distance") / 25) * 25).alias("score_distance_bucket"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

rollup_base = (
    tagged
    .groupBy("enrolled_at_hour", "grade_status")
    .agg(
        F.round(F.sum("score_distance") / F.nullif(F.sum("score_distance_bucket"), F.lit(0)), 3).alias("score_distance_to_score_distance_bucket_ratio"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.min("enrolled_at_hour").alias("min_enrolled_at_hour"),
    )
)

per_label = (
    rollup_base
    .select(
        "enrolled_at_hour",
        "grade_status",
        "score_distance_to_score_distance_bucket_ratio",
        "min_enrolled_at_hour",
        F.explode_outer("term_list").alias("term_list_item"),
    )
)

result = (
    per_label
    .select(
        "enrolled_at_hour",
        "grade_status",
        "score_distance_to_score_distance_bucket_ratio",
        "term_list_item",
        F.coalesce(F.col("enrolled_at_hour"), F.lit(0)).alias("enrolled_at_hour_filled"),
        F.abs(F.col("min_enrolled_at_hour") - 10).alias("min_enrolled_at_hour_distance"),
    )
    .orderBy(F.col("grade_status").desc(), F.col("term_list_item").desc())
)
