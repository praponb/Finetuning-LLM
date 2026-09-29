from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("course_id") <= 3)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_2"),
    )
)

summarized_rows = (
    scoped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.last_enrollment_enrolled_at_2")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_3"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows_2 = (
    summarized_rows.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at_3",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enrollment_stats_3 = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.sum("score").alias("total_enrollment_score_2"),
    )
)

profiled = (
    summarized_rows_2.alias("s")
    .join(enrollment_stats_3.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        "e.first_enrollment_enrolled_at",
        "e.total_enrollment_score_2",
    )
)

result = (
    profiled
    .groupingSets(
        [["enrollment_count", "course_id", "student_id"], ["enrollment_count"]],
        "enrollment_count", "course_id", "student_id",
    )
    .agg(
        F.count(F.when(F.col("term").isNotNull(), 1)).alias("n_term_is_not_null"),
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
    )
)
