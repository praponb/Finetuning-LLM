from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
    )
)

tagged = (
    recent_enrollments
    .select(
        "id",
        "score",
        "grade_status",
        "score_adj",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        (F.col("score") - F.col("score_adj")).alias("score_minus_score_adj"),
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
)

screened = (
    tagged
    .crossJoin(tagged.agg(F.min("score_adj").alias("overall_min_score_adj")))
    .filter(F.col("score_adj") > F.col("overall_min_score_adj"))
    .drop("overall_min_score_adj")
)

annotated = (
    screened
    .select(
        "id",
        "score",
        "grade_status",
        "score_adj",
        "grade_status_or_unknown",
        "score_minus_score_adj",
        "term_ref",
        F.round(F.col("score") / F.nullif(F.col("score_minus_score_adj"), F.lit(0)), 3).alias("score_per_score_minus_score_adj"),
        F.when(F.col("score") >= 74.9, "A").when(F.col("score") >= 35.8, "B").otherwise("C").alias("score_tier"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown_2"),
    )
)

result = (
    annotated
    .groupBy("grade_status")
    .pivot("score_tier", ["B", "C", "A"])
    .agg(F.max("score"))
    .orderBy("grade_status")
)
