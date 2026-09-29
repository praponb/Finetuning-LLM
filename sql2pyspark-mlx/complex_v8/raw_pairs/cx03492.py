from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-12 09:00:00").cast("timestamp")) & (F.col("grade_status").isin("FAIL", "INCOMPLETE")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("term"), F.lit("UNKNOWN")).alias("term_or_unknown"),
    )
)

scored = (
    cleaned_enrollments
    .filter(F.col("term_or_unknown") == "2025T2")
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "term_or_unknown",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.initcap(F.trim(F.col("term_or_unknown"))).alias("term_or_unknown_clean"),
    )
)

scored_2 = (
    scored
    .select(
        "id",
        "course_id",
        "grade_status",
        "enrolled_at",
        "term_or_unknown",
        "enrolled_at_dayofweek",
        "term_or_unknown_clean",
        F.concat(F.col("term_or_unknown_clean"), F.lit("#"), F.col("course_id").cast("string")).alias("term_or_unknown_clean_ref"),
    )
)

enriched = (
    scored_2
    .filter(F.hour(F.col("enrolled_at")).between(8, 10))
    .select(
        "*",
        F.when(F.col("grade_status").isin("PASS", "WITHDRAWN"), "group_a").when(F.col("grade_status") == "INCOMPLETE", "group_b").otherwise("other").alias("grade_status_group"),
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
        F.substring(F.trim(F.col("term_or_unknown")), 1, 3).alias("term_or_unknown_prefix"),
    )
)

summary = (
    enriched
    .groupBy("grade_status_group", "grade_status", "course_id")
    .agg(
        F.round(F.stddev("enrolled_at_dayofweek"), 2).alias("stddev_enrolled_at_dayofweek"),
        F.round(100 * F.count(F.when(F.col("grade_status").isNotNull(), 1)) / F.count("*"), 1).alias("pct_grade_status_is_not_null"),
        F.sum("enrolled_at_dayofweek").alias("total_enrolled_at_dayofweek"),
        F.count("*").alias("row_count"),
        F.percentile_approx("enrolled_at_dayofweek", 0.5).alias("median_enrolled_at_dayofweek"),
    )
)

result = (
    summary
    .select(
        "grade_status_group",
        "grade_status",
        "course_id",
        "stddev_enrolled_at_dayofweek",
        "pct_grade_status_is_not_null",
        "row_count",
        F.initcap(F.trim(F.col("grade_status_group"))).alias("grade_status_group_clean"),
        F.coalesce(F.col("row_count"), F.lit(0)).alias("row_count_filled"),
    )
    .orderBy(
        F.col("grade_status").asc(),
        F.col("grade_status_group").asc_nulls_last(),
    )
)
