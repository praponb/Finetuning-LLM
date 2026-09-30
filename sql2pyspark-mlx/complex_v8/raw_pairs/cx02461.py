from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 38.1) & (F.col("grade_status") == "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
        F.concat_ws("-", F.col("term"), F.col("grade_status")).alias("term_grade_status_label"),
    )
)

totals = (
    filtered_enrollments
    .groupBy("grade_status_group")
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
        F.countDistinct("grade_status_or_unknown").alias("distinct_grade_status_or_unknowns"),
        F.count("*").alias("cnt"),
        F.max("score").alias("max_score"),
        F.min("enrolled_at").alias("first_enrolled_at"),
    )
)

labeled = (
    totals
    .select(
        "*",
        F.when(F.col("grade_status_group").isin("other", "group_b"), "group_a").when(F.col("grade_status_group") == "group_a", "group_b").otherwise("other").alias("grade_status_group_group"),
    )
)

screened = (
    labeled
    .crossJoin(labeled.agg(F.avg("cnt").alias("overall_avg_cnt")))
    .filter(F.col("cnt") > F.col("overall_avg_cnt"))
    .drop("overall_avg_cnt")
)

derived = (
    screened
    .filter(F.col("first_enrolled_at") >= F.lit("2025-01-19 11:00:00").cast("timestamp"))
    .select(
        "grade_status_group",
        "n_term_is_not_null",
        "distinct_grade_status_or_unknowns",
        "cnt",
        "grade_status_group_group",
        F.date_trunc("day", F.col("first_enrolled_at")).alias("first_enrolled_at_day"),
    )
)

result = (
    derived
    .filter(F.col("n_term_is_not_null") > 3)
    .select(
        "grade_status_group",
        "n_term_is_not_null",
        "distinct_grade_status_or_unknowns",
        "cnt",
        "grade_status_group_group",
        "first_enrolled_at_day",
        F.when(F.col("cnt") >= 8, "large").when(F.col("cnt") >= 4, "medium").otherwise("small").alias("cnt_tier"),
    )
    .orderBy(F.col("cnt_tier").asc_nulls_last(), "distinct_grade_status_or_unknowns")
)
