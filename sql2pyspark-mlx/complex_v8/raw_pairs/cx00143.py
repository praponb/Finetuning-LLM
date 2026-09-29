from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("score") <= 90.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("term").isin("2025T2", "2024T1"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
        (F.floor(F.col("score") / 25) * 25).alias("score_bucket"),
        F.date_trunc("quarter", F.col("enrolled_at")).alias("enrolled_at_quarter"),
    )
)

tagged = (
    filtered_enrollments
    .filter(F.col("term_group").isNotNull())
    .select(
        "*",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

result = (
    tagged
    .rollup("term_or_unknown", "course_id", "grade_status")
    .agg(
        F.grouping("term_or_unknown").alias("is_total_term_or_unknown"),
        F.sum("score_bucket").alias("total_score_bucket"),
        F.count(F.when(F.col("term") == "2025T2", 1)).alias("n_term_2025t2"),
    )
    .orderBy(
        F.col("term_or_unknown").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
