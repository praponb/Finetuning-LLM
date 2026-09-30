from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("grade_status").isin("WITHDRAWN", "PASS", "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

with_activity = (
    scoped
    .crossJoin(scoped.agg(F.avg("score").alias("overall_avg_score")))
    .filter(F.col("score") > F.col("overall_avg_score"))
    .drop("overall_avg_score")
)

result = (
    with_activity
    .groupingSets(
        [["grade_status", "term_len"], ["grade_status"], ["term_len"]],
        "grade_status", "term_len",
    )
    .agg(
        F.round(F.sum("score") / F.nullif(F.sum("term_len"), F.lit(0)), 3).alias("score_to_term_len_ratio"),
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("term_len").asc_nulls_last(),
    )
)
