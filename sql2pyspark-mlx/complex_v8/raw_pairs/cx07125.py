from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") >= 20.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("INCOMPLETE", "PASS"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

scored = (
    recent_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        "grade_status_len",
        "score_filled",
        F.coalesce(F.col("grade_status_len"), F.lit(0)).alias("grade_status_len_filled"),
    )
)

grade_status_stats = (
    scored
    .groupBy("grade_status")
    .agg(F.max("grade_status_len").alias("max_grade_status_len"))
)

outliers = (
    scored.alias("s")
    .join(grade_status_stats.alias("g"), F.col("g.grade_status") == F.col("s.grade_status"), "inner")
    .filter(F.col("s.grade_status_len") == F.col("g.max_grade_status_len"))
    .select(
        "s.*",
        "g.max_grade_status_len",
        F.round(F.col("s.grade_status_len") - F.col("g.max_grade_status_len"), 2).alias("grade_status_len_minus_max"),
    )
)

result = (
    outliers
    .cube("grade_status_len", "grade_status")
    .agg(
        F.grouping("grade_status_len").alias("is_total_grade_status_len"),
        F.round(F.sum("grade_status_len_minus_max") / F.nullif(F.sum("grade_status_len_filled"), F.lit(0)), 3).alias("grade_status_len_minus_max_to_grade_status_len_f"),
        F.countDistinct("term").alias("distinct_terms"),
    )
    .select(
        "grade_status_len",
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "is_total_grade_status_len",
        "grade_status_len_minus_max_to_grade_status_len_f",
        "distinct_terms",
    )
    .orderBy(
        F.col("grade_status_len").asc_nulls_last(),
        F.col("grade_status").asc_nulls_last(),
    )
)
