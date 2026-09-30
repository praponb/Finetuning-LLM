from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("score").between(20.2, 78.8)) | (F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

labeled = (
    valid_enrollments
    .select(
        "*",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.concat(F.col("term"), F.lit("#"), F.col("id").cast("string")).alias("term_ref"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

tagged = (
    labeled
    .filter(F.col("term") == "2025T2")
    .select(
        "id",
        "course_id",
        "term",
        "enrolled_at",
        "term_ref",
        "grade_status_or_unknown",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
        F.coalesce(F.col("score_bucket"), F.lit(0)).alias("score_bucket_filled"),
        F.abs(F.col("score_bucket") - 65).alias("score_bucket_distance"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") > 17.4), "left_semi")
)

result = (
    with_activity
    .select(
        "id",
        F.col("score_bucket_distance").alias("m_score_bucket_distance"),
        F.col("score_bucket_filled").alias("score_bucket_filled_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_score_bucket_distance", "score_bucket_filled_v"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
)
