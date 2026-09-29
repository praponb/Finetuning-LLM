from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 39.4).alias("score_distance"),
        (F.col("enrolled_at") + F.expr("INTERVAL 30 MINUTES")).alias("enrolled_at_plus_30m"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count(F.when(~F.col("grade_status").isin("PASS", "WITHDRAWN"), 1)).alias("enrollment_grade_status_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

with_activity = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (~F.col("e.grade_status").isin("FAIL", "WITHDRAWN")) & (F.to_date(F.col("e.enrolled_at")) < F.lit("2025-02-06").cast("date")), "left_semi")
)

result = (
    with_activity
    .filter(F.col("grade_status") != "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "enrolled_at_plus_30m",
        "enrollment_count",
        "enrollment_grade_status_count",
    )
    .orderBy(F.col("enrolled_at_plus_30m").asc(), F.col("student_id").desc(), "id")
    .limit(20)
)
