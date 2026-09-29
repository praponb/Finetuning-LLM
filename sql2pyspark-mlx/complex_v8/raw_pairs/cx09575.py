from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("term").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at",
    )
)

enriched = (
    filtered_enrollments
    .select(
        "*",
        F.when(F.col("term").isin("2025T1", "2024T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.substring(F.trim(F.col("term")), 1, 2).alias("term_prefix"),
    )
)

result = (
    enriched
    .groupingSets(
        [["term", "course_id", "student_id"], ["term"], []],
        "term", "course_id", "student_id",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.min("score").alias("min_score"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("term").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
