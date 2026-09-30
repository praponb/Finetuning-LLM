from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") != "FAIL") & (F.col("term").isin("2024T1", "2024T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

screened = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.term").isin("2025T1", "2024T2")) & (F.col("e.score").between(11.1, 66.0)), "left_semi")
    .filter(F.col("grade_status") != "WITHDRAWN")
)

stats = (
    screened
    .filter(F.col("student_id") >= 10)
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "grade_status",
        "grade_status_or_unknown",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.percentile_approx("score", 0.5).alias("median_score"),
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.sum("score").alias("sum_score_check"),
    )
    .filter(F.col("sum_score_check") > 8.8)
    .drop("sum_score_check")
)

result = (
    stats
    .select(
        "enrolled_at_month",
        "grade_status_or_unknown",
        "distinct_ids",
        "total_score",
        "median_score",
        F.concat(F.col("grade_status_or_unknown"), F.lit("#"), F.col("distinct_ids").cast("string")).alias("grade_status_or_unknown_ref"),
    )
)
