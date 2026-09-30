from pyspark.sql import functions as F

base_enrollments = (
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

annotated = (
    base_enrollments
    .select(
        "*",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
        F.abs(F.col("score") - 32.9).alias("score_distance"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("score").alias("max_enrollment_score"))
)

with_counts = (
    annotated.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.max_enrollment_score")
)

result = (
    with_counts
    .rollup("grade_status", "student_id")
    .agg(
        F.grouping("grade_status").alias("is_total_grade_status"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .select(
        F.coalesce(F.col("grade_status"), F.lit("ALL")).alias("grade_status"),
        "student_id",
        "is_total_grade_status",
        "term_list",
    )
    .orderBy(
        F.col("grade_status").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
