from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score") > 32.7) & (F.col("student_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("term").isin("2025T1", "2024T2"), "group_a").when(F.col("term") == "2024T1", "group_b").otherwise("other").alias("term_group"),
    )
)

filtered = (
    prepared_enrollments
    .crossJoin(prepared_enrollments.agg(F.max("score").alias("overall_max_score")))
    .filter(F.col("score") < F.col("overall_max_score"))
    .drop("overall_max_score")
)

rollup_base = (
    filtered
    .groupBy(
        "term_group",
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "student_id",
    )
    .agg(
        F.round(100 * F.count(F.when(F.col("term").isin("2024T2", "2024T1", "2025T1"), 1)) / F.count("*"), 1).alias("pct_term_in_2024t2_2024t1_2025t1"),
        F.sum(F.when(F.col("grade_status") == "INCOMPLETE", F.col("score")).otherwise(0)).alias("score_grade_status_incomplete"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.avg("score").alias("avg_score_check"),
    )
    .filter(F.col("avg_score_check") > 34.2)
    .drop("avg_score_check")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

summarized_rows = (
    rollup_base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

scored = (
    summarized_rows
    .filter(F.size(F.col("grade_status_list")) >= 2)
    .select(
        "*",
        F.date_trunc("week", F.col("enrolled_at_month")).alias("enrolled_at_month_week"),
        F.lower(F.trim(F.col("term_group"))).alias("term_group_clean"),
        F.array_join(F.array_sort(F.col("grade_status_list")), ",").alias("grade_status_list_text"),
    )
)

result = (
    scored
    .cube("term_group", "student_id")
    .agg(
        F.min("enrolled_at_month_week").alias("first_enrolled_at_month_week"),
        F.max("pct_term_in_2024t2_2024t1_2025t1").alias("max_pct_term_in_2024t2_2024t1_2025t1"),
        F.round(F.sum("score_grade_status_incomplete") / F.nullif(F.sum("pct_term_in_2024t2_2024t1_2025t1"), F.lit(0)), 3).alias("score_grade_status_incomplete_to_pct_term_in_202"),
    )
    .orderBy(
        F.col("term_group").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
