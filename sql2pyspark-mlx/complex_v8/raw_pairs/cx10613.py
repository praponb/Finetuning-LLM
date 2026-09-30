from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

labeled = (
    scoped_enrollments
    .filter(F.col("term").isin("2024T1", "2025T1"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.length(F.trim(F.col("term"))).alias("term_len"),
        F.when(F.col("grade_status").isin("WITHDRAWN", "PASS"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.count("*").alias("enrollment_count_3"),
    )
)

summarized_rows = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        F.coalesce(F.col("e.enrollment_count_3"), F.lit(0)).alias("enrollment_count_3"),
    )
)

result = (
    summarized_rows
    .select("id", "term_len", "enrollment_count_2")
    .unpivot(
        ids=["id"],
        values=["term_len", "enrollment_count_2"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
)
