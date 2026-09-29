from pyspark.sql import functions as F

base_enrollments = df_enrollments.filter(F.col("grade_status").isNotNull())

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count("*").alias("enrollment_count_2"),
    )
)

with_counts = (
    base_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
    )
)

qualified = (
    with_counts.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.score") < 71.0) & (~F.col("e.grade_status").isin("FAIL", "PASS")), "left_semi")
    .filter(F.col("enrollment_count").between(2, 6))
)

screened = (
    qualified.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.score") < 62.5), "left_anti")
    .filter(F.col("enrolled_at") >= F.lit("2025-01-25 14:00:00").cast("timestamp"))
)

result = (
    screened
    .rollup("course_id", "enrollment_count_2", "term")
    .agg(
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
        F.countDistinct("course_id").alias("distinct_course_ids"),
    )
    .orderBy(
        F.col("course_id").asc_nulls_last(),
        F.col("enrollment_count_2").asc_nulls_last(),
        F.col("term").asc_nulls_last(),
    )
)
