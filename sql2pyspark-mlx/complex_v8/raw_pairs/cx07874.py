from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.col("enrolled_at") < F.lit("2025-02-24 13:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2024T1", "2025T1"), "group_a").when(F.col("term") == "2025T2", "group_b").otherwise("other").alias("term_group"),
        F.when(F.col("score") >= 78.8, "HIGH").when(F.col("score") >= 34.2, "MEDIUM").otherwise("LOW").alias("score_tier"),
        F.date_trunc("day", F.col("enrolled_at")).alias("enrolled_at_day"),
    )
)

with_details_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.enrolled_at",
        "s.term_group",
        "s.score_tier",
        "s.enrolled_at_day",
        F.col("c.credits").alias("course_credits"),
    )
)

result = (
    with_details_course
    .groupingSets(
        [["term_group", "score_tier", "course_credits"], ["term_group"], ["score_tier"], []],
        "term_group", "score_tier", "course_credits",
    )
    .agg(
        F.countDistinct("term").alias("distinct_terms"),
        F.count(F.when(F.col("term").isin("2025T2", "2024T2"), 1)).alias("n_term_in_2025t2_2024t2"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
    .orderBy(
        F.col("term_group").asc_nulls_last(),
        F.col("score_tier").asc_nulls_last(),
        F.col("course_credits").asc_nulls_last(),
    )
)
