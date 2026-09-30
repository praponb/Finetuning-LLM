from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) | (F.col("score") >= 20.6))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

rollup_base = (
    scoped
    .groupBy("grade_status", "grade_status_group", "term")
    .agg(
        F.count(F.when(F.col("grade_status") == "INCOMPLETE", 1)).alias("n_grade_status_incomplete"),
        F.sum("score").alias("total_score"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.count("*").alias("row_count"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", F.col("score")).otherwise(0)).alias("score_grade_status_incomplete"),
    )
)

with_activity = (
    rollup_base
    .crossJoin(rollup_base.agg(F.avg("score_grade_status_incomplete").alias("overall_avg_score_grade_status_incomplete")))
    .filter(F.col("score_grade_status_incomplete") > F.col("overall_avg_score_grade_status_incomplete"))
    .drop("overall_avg_score_grade_status_incomplete")
)

tagged = (
    with_activity
    .select(
        "grade_status",
        "grade_status_group",
        "term",
        "total_score",
        "distinct_grade_status",
        "distinct_course_ids",
        "score_grade_status_incomplete",
        (F.col("distinct_grade_status") - F.col("total_score")).alias("distinct_grade_status_minus_total_score"),
        F.coalesce(F.col("row_count"), F.lit(0)).alias("row_count_filled"),
    )
)

enriched = (
    tagged
    .select(
        "*",
        F.substring(F.trim(F.col("grade_status_group")), 1, 2).alias("grade_status_group_prefix"),
    )
)

with_activity_2 = (
    enriched
    .crossJoin(enriched.agg(F.min("distinct_grade_status").alias("overall_min_distinct_grade_status")))
    .filter(F.col("distinct_grade_status") > F.col("overall_min_distinct_grade_status"))
    .drop("overall_min_distinct_grade_status")
)

result = (
    with_activity_2
    .filter(F.col("total_score") >= 240.0)
    .select(
        "grade_status",
        "grade_status_group",
        "term",
        "distinct_grade_status",
        "distinct_course_ids",
        "score_grade_status_incomplete",
        "distinct_grade_status_minus_total_score",
        "row_count_filled",
        "grade_status_group_prefix",
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
        F.round(F.col("total_score") * 1.07, 2).alias("total_score_adj"),
    )
    .orderBy(
        F.col("score_grade_status_incomplete").asc(),
        F.col("term").desc(),
        "grade_status",
        "grade_status_group",
    )
    .limit(10)
)
