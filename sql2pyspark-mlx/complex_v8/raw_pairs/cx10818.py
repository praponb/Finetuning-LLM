from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

annotated = (
    filtered_enrollments
    .select(
        "id",
        "term",
        "score",
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.upper(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.round(F.col("score") * 1.1, 2).alias("score_adj"),
    )
)

without_matches = (
    annotated
    .crossJoin(annotated.agg(F.min("score").alias("overall_min_score")))
    .filter(F.col("score") > F.col("overall_min_score"))
    .drop("overall_min_score")
)

result = (
    without_matches
    .select(
        "id",
        "score",
        "score_bucket",
        "score_adj",
        F.length(F.trim(F.col("grade_status_clean"))).alias("grade_status_clean_len"),
    )
    .orderBy("score_bucket", F.col("id").asc_nulls_last())
    .limit(10)
)
