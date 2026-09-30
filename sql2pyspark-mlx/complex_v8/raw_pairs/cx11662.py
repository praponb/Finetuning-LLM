from pyspark.sql import functions as F

valid = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("course_id").cast("string")).alias("term_ref"),
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

enriched_course = (
    valid.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", F.col("c.credits").alias("course_credits"))
)

rollup_base = (
    enriched_course
    .groupBy("term", F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.max("score").alias("max_score"),
        F.count(F.when(F.col("term") == "2025T2", 1)).alias("n_term_2025t2"),
        F.round(F.sum("score_adj") / F.nullif(F.sum("score"), F.lit(0)), 3).alias("score_adj_to_score_ratio"),
        F.avg("course_credits").alias("avg_course_credits_check"),
    )
    .filter(F.col("avg_course_credits_check") > 2)
    .drop("avg_course_credits_check")
)

without_matches = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("max_score").alias("overall_avg_max_score")))
    .filter(F.col("max_score") > F.col("overall_avg_max_score"))
    .drop("overall_avg_max_score")
)

enriched = (
    without_matches
    .select(
        "*",
        F.date_format(F.col("enrolled_at_month"), "MMM yyyy").alias("enrolled_at_month_label"),
        F.date_trunc("quarter", F.col("enrolled_at_month")).alias("enrolled_at_month_quarter"),
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

result = (
    enriched
    .filter(F.col("score_adj_to_score_ratio") > 4.0)
    .select(
        "term",
        "enrolled_at_month",
        "n_term_2025t2",
        "score_adj_to_score_ratio",
        "enrolled_at_month_label",
        "enrolled_at_month_quarter",
        "term_or_unknown",
        F.when(F.col("score_adj_to_score_ratio") >= 5.5, "A").when(F.col("score_adj_to_score_ratio") >= 2.5, "B").otherwise("C").alias("score_adj_to_score_ratio_tier"),
    )
    .orderBy(F.col("enrolled_at_month_quarter").asc())
)
