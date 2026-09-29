from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("id").cast("string")).alias("grade_status_ref"),
    )
)

without_matches = (
    cleaned_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")), "left_anti")
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

profiled = (
    without_matches.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

summarized_rows = (
    profiled.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

result = (
    summarized_rows
    .select(
        "id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "first_enrollment_enrolled_at",
        F.round(F.col("score") * 0.9, 2).alias("score_adj"),
        F.lower(F.trim(F.col("grade_status_ref"))).alias("grade_status_ref_clean"),
    )
    .orderBy(F.col("grade_status").asc_nulls_last())
)
