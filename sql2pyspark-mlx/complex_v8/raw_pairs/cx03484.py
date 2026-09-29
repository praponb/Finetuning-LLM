from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "WITHDRAWN") & (F.col("term") == "2024T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 76.8, "HIGH").when(F.col("score") >= 39.4, "MEDIUM").otherwise("LOW").alias("score_tier"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

grouped = (
    prepared_enrollments
    .groupBy("term")
    .agg(
        F.round(F.avg("score_bucket"), 2).alias("avg_score_bucket"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("n_term_2024t1"),
    )
)

without_matches = (
    grouped
    .crossJoin(grouped.agg(F.avg("n_term_2024t1").alias("overall_avg_n_term_2024t1")))
    .filter(F.col("n_term_2024t1") > F.col("overall_avg_n_term_2024t1"))
    .drop("overall_avg_n_term_2024t1")
)

result = (
    without_matches
    .filter(F.col("avg_score_bucket") < 60.0)
    .select(
        "term",
        "avg_score_bucket",
        (F.col("n_term_2024t1") - F.col("avg_score_bucket")).alias("n_term_2024t1_minus_avg_score_bucket"),
        F.when(F.col("row_count") >= 8, "large").when(F.col("row_count") >= 4, "medium").otherwise("small").alias("row_count_tier"),
    )
    .orderBy(F.col("row_count_tier").desc(), F.col("term").asc())
)
