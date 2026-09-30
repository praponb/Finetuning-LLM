from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("student_id") <= 16)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.when(F.col("term").isin("2025T2", "2024T1"), "group_a").when(F.col("term") == "2025T1", "group_b").otherwise("other").alias("term_group"),
    )
)

scored = (
    scoped
    .withColumn("term_group_ref", F.concat(F.col("term_group"), F.lit("#"), F.col("student_id").cast("string")))
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.count(F.when(~F.col("grade_status").isin("PASS", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
    )
)

with_stats = (
    scored.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.avg_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    with_stats
    .groupingSets(
        [["grade_status", "student_id", "course_id"], ["grade_status"]],
        "grade_status", "student_id", "course_id",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.percentile_approx("enrollment_count", 0.5).alias("median_enrollment_count"),
    )
)
