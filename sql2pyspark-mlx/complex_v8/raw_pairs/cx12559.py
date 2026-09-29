from pyspark.sql import functions as F

recent = (
    df_enrollments
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(": ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
    )
)

with_stats = (
    recent.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.min_enrollment_score",
        "e.first_enrollment_enrolled_at",
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(~F.col("grade_status").isin("INCOMPLETE", "FAIL"), 1)).alias("enrollment_grade_status_count"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("enrollment_grade_status_count_2"),
    )
)

with_stats_2 = (
    with_stats.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        F.coalesce(F.col("e.enrollment_grade_status_count_2"), F.lit(0)).alias("enrollment_grade_status_count_2"),
    )
)

result = (
    with_stats_2
    .cube("student_id", "enrollment_grade_status_count_2")
    .agg(
        F.grouping("student_id").alias("is_total_student_id"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .orderBy(
        F.col("student_id").asc_nulls_last(),
        F.col("enrollment_grade_status_count_2").asc_nulls_last(),
    )
)
