from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") != "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

course_id_stats = (
    base_enrollments
    .groupBy("course_id")
    .agg(F.avg("score").alias("avg_score"))
)

outliers = (
    base_enrollments.alias("s")
    .join(course_id_stats.alias("g"), F.col("g.course_id") == F.col("s.course_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

result = (
    outliers
    .groupingSets(
        [["course_id", "term", "student_id"], ["course_id"], ["term"]],
        "course_id", "term", "student_id",
    )
    .agg(
        F.grouping("course_id").alias("is_total_course_id"),
        F.countDistinct("term").alias("distinct_terms"),
        F.round(F.sum("score") / F.nullif(F.sum("avg_score"), F.lit(0)), 3).alias("score_to_avg_score_ratio"),
        F.round(F.sum("avg_score"), 2).alias("total_avg_score"),
    )
    .select(
        "course_id",
        F.coalesce(F.col("term"), F.lit("ALL")).alias("term"),
        "student_id",
        "is_total_course_id",
        "distinct_terms",
        "score_to_avg_score_ratio",
        "total_avg_score",
    )
)
