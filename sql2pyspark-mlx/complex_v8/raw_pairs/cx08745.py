from pyspark.sql import functions as F

prepared = (
    df_enrollments
    .filter((~F.col("term").isin("2025T1", "2025T2")) & (F.col("grade_status").isin("WITHDRAWN", "FAIL", "INCOMPLETE")))
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status").isin("PASS", "INCOMPLETE"), 1)).alias("enrollment_grade_status_count"),
    )
)

profiled = (
    prepared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    profiled
    .groupBy("course_id")
    .pivot("grade_status", ["PASS", "WITHDRAWN", "INCOMPLETE", "FAIL"])
    .agg(F.min("score"))
    .orderBy("course_id")
)
