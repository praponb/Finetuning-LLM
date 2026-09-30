from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("term") == "2025T1") & (F.month(F.col("enrolled_at")).isin(1, 2)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

with_activity = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
    .filter(F.col("term") == "2024T1")
)

enriched = (
    with_activity
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "term",
        "score",
        F.when(F.col("score") >= 58.8, "large").when(F.col("score") >= 35.8, "medium").otherwise("small").alias("score_tier"),
    )
)

annotated = (
    enriched
    .select(
        "id",
        "student_id",
        "term",
        "score",
        "score_tier",
        F.when(F.col("term").isin("2024T2", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
    )
)

result = (
    annotated
    .cube("score_tier", "term_group", "term")
    .agg(
        F.sum(F.when(F.col("term") != "2024T2", 1).otherwise(0)).alias("n_term_2024t2"),
        F.round(F.stddev("score"), 2).alias("stddev_score"),
    )
)
