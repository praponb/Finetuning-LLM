from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("term").isin("2024T1", "2025T1", "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.regexp_replace(F.trim(F.col("term")), " ", "_")).alias("term_slug"),
        F.when(F.col("grade_status").isin("WITHDRAWN", "FAIL"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

student_id_stats = (
    cleaned_enrollments
    .groupBy("student_id")
    .agg(F.avg("score").alias("avg_score"))
)

above_average = (
    cleaned_enrollments.alias("s")
    .join(student_id_stats.alias("g"), F.col("g.student_id") == F.col("s.student_id"), "inner")
    .filter(F.col("s.score") > F.col("g.avg_score"))
    .select(
        "s.*",
        "g.avg_score",
        F.round(F.col("s.score") - F.col("g.avg_score"), 2).alias("score_minus_avg"),
    )
)

summary = (
    above_average
    .filter(F.col("score_minus_avg") < 10.0)
    .groupBy("grade_status_group", "student_id", "term")
    .agg(
        F.count("*").alias("n_rows"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
        F.countDistinct("grade_status").alias("distinct_grade_status"),
        F.round(F.sum("score") / F.nullif(F.sum("score_minus_avg"), F.lit(0)), 3).alias("score_to_score_minus_avg_ratio"),
    )
)

filtered = (
    summary.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left_semi")
    .filter(F.col("distinct_grade_status") < 8)
)

result = (
    filtered
    .groupBy("student_id", "distinct_grade_status")
    .pivot("grade_status_group", ["group_a", "group_b", "other"])
    .agg(F.max("n_rows").alias("max_v"), F.sum("n_rows").alias("sum_v"))
)
