from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((~F.col("term").isin("2024T1", "2025T1")) | (F.col("score") > 8.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

annotated = (
    recent
    .filter(F.col("term_prefix").isNotNull())
    .select(
        "id",
        "course_id",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term_prefix")), " ", "_")).alias("term_prefix_slug"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
    )
)

tagged = (
    annotated
    .filter(F.col("term_prefix_slug").isNotNull())
    .select(
        "*",
        F.concat_ws(" / ", F.col("term_prefix_slug"), F.col("grade_status")).alias("term_prefix_slug_grade_status_label"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

with_counts = (
    tagged.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.last_enrollment_enrolled_at",
    )
)

result = (
    with_counts
    .rollup("course_id", "grade_status", "enrollment_count")
    .agg(
        F.round(F.sum("score"), 2).alias("total_score"),
        F.round(F.sum("enrollment_count") / F.nullif(F.sum("score_adj"), F.lit(0)), 3).alias("enrollment_count_to_score_adj_ratio"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
        F.col("enrollment_count").asc_nulls_last(),
    )
)
