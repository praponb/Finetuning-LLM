from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.when(F.col("score") >= 73.7, "HIGH").when(F.col("score") >= 35.8, "MEDIUM").otherwise("LOW").alias("score_tier"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

summarized_rows = (
    valid_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

term_stats = (
    summarized_rows
    .groupBy("term")
    .agg(F.avg("score_bucket").alias("avg_score_bucket"))
)

compared = (
    summarized_rows.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score_bucket") > F.col("g.avg_score_bucket"))
    .select(
        "s.*",
        "g.avg_score_bucket",
        F.round(F.col("s.score_bucket") - F.col("g.avg_score_bucket"), 2).alias("score_bucket_minus_avg"),
    )
)

result = (
    compared
    .groupingSets(
        [["score_tier", "student_id", "term"], ["score_tier"], []],
        "score_tier", "student_id", "term",
    )
    .agg(
        F.round(F.avg("score_bucket"), 2).alias("avg_score_bucket_2"),
        F.sum("score_bucket").alias("total_score_bucket"),
    )
    .select(
        F.coalesce(F.col("score_tier"), F.lit("ALL")).alias("score_tier"),
        "student_id",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "avg_score_bucket_2",
        "total_score_bucket",
    )
    .orderBy(
        F.col("score_tier").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
