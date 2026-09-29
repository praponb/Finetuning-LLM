from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score") < 83.1) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
    )
)

with_activity = (
    prepared_enrollments
    .crossJoin(prepared_enrollments.agg(F.avg("score").alias("overall_avg_score")))
    .filter(F.col("score") > F.col("overall_avg_score"))
    .drop("overall_avg_score")
)

derived = (
    with_activity
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("term_slug")), " ", "_")).alias("term_slug_slug"),
    )
)

term_stats = (
    derived
    .groupBy("term")
    .agg(F.avg("score").alias("avg_score"))
)

vs_group = (
    derived.alias("s")
    .join(term_stats.alias("g"), F.col("g.term") == F.col("s.term"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

result = (
    vs_group
    .groupingSets(
        [["term", "course_id"], ["term"], []],
        "term", "course_id",
    )
    .agg(
        F.grouping("term").alias("is_total_term"),
        F.round(F.sum("score") / F.nullif(F.sum("score_minus_avg"), F.lit(0)), 3).alias("score_to_score_minus_avg_ratio"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("term").asc_nulls_last(), F.col("course_id").asc_nulls_last())
)
