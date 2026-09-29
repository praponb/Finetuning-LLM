from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("score") > 35.8) & (F.col("grade_status") != "WITHDRAWN"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

without_matches = (
    prepared_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-08 10:00:00").cast("timestamp")), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("score").alias("min_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("score").alias("min_enrollment_score_2"),
    )
)

with_counts = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.min_enrollment_score",
        "e.first_enrollment_enrolled_at",
        "e.min_enrollment_score_2",
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
    )
)

profiled = (
    with_counts.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["term", "enrollment_count"], ["term"]],
        "term", "enrollment_count",
    )
    .agg(
        F.sum(F.when(~F.col("grade_status").isin("WITHDRAWN", "PASS"), F.col("enrollment_term_count")).otherwise(0)).alias("enrollment_term_count_grade_status_not_in_withdr"),
        F.max("enrolled_at").alias("last_enrolled_at"),
    )
)
