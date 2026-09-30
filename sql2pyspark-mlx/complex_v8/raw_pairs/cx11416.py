from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("score") > 26.2)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

derived = (
    scoped
    .select(
        "id",
        "student_id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "term_grade_status_label",
        "enrolled_at_plus_30m",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled_2"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled_3"),
    )
)

annotated = (
    derived
    .select(
        "*",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
        F.when(F.col("score_filled_2") >= 55.0, "HIGH").when(F.col("score_filled_2") >= 30.0, "MEDIUM").otherwise("LOW").alias("score_filled_2_tier"),
    )
)

course_id_stats = (
    annotated
    .groupBy("course_id")
    .agg(F.avg("grade_status_len").alias("avg_grade_status_len"))
)

compared = (
    annotated.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.grade_status_len") > F.col("g.avg_grade_status_len"))
    .select(
        "s.*",
        "g.avg_grade_status_len",
        F.round(F.col("s.grade_status_len") - F.col("g.avg_grade_status_len"), 2).alias("grade_status_len_minus_avg"),
    )
)

qualified = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.term") == "2025T2") & (F.col("e.grade_status") != "PASS"), "left_semi")
)

annotated_2 = (
    qualified
    .select(
        "id",
        "grade_status",
        "term_grade_status_label",
        "enrolled_at_plus_30m",
        "score_filled",
        "score_filled_2",
        "score_filled_3",
        "grade_status_len",
        "score_filled_2_tier",
        "grade_status_len_minus_avg",
        F.concat(F.col("score_filled_2_tier"), F.lit("#"), F.col("student_id").cast("string")).alias("score_filled_2_tier_ref"),
        F.when(F.col("score_filled_2_tier").isin("MEDIUM", "HIGH"), "group_a").when(F.col("score_filled_2_tier") == "LOW", "group_b").otherwise("other").alias("score_filled_2_tier_group"),
    )
)

score_filled_2_tier_group_stats = (
    annotated_2
    .groupBy("score_filled_2_tier_group")
    .agg(F.avg("score_filled_2").alias("avg_score_filled_2"))
)

outliers = (
    annotated_2.alias("s")
    .join(score_filled_2_tier_group_stats.alias("g"), F.col("g.score_filled_2_tier_group") == F.col("s.score_filled_2_tier_group"), "inner")
    .filter(F.col("s.score_filled_2") > F.col("g.avg_score_filled_2"))
    .select(
        "s.*",
        "g.avg_score_filled_2",
        F.round(F.col("s.score_filled_2") - F.col("g.avg_score_filled_2"), 2).alias("score_filled_2_minus_avg"),
    )
)

result = (
    outliers
    .select(
        "id",
        "grade_status",
        "term_grade_status_label",
        "enrolled_at_plus_30m",
        "score_filled",
        "score_filled_3",
        "score_filled_2_tier",
        "score_filled_2_tier_ref",
        "score_filled_2_tier_group",
        "score_filled_2_minus_avg",
        F.greatest(F.col("score_filled_2"), F.col("score_filled_2_minus_avg")).alias("greatest_score_filled_2_score_filled_2_minus_avg"),
        F.date_format(F.col("enrolled_at_plus_30m"), "yyyy-MM-dd").alias("enrolled_at_plus_30m_label"),
    )
    .orderBy(
        F.col("score_filled_3").asc_nulls_last(),
        F.col("grade_status").desc_nulls_first(),
    )
)
