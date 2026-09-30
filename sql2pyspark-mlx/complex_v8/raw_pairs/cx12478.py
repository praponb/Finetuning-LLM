from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) & (F.hour(F.col("enrolled_at")).between(8, 13)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.lower(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
    )
)

with_activity = (
    recent.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.grade_status") == "INCOMPLETE"), "left_semi")
)

filtered = (
    with_activity.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-07 11:00:00").cast("timestamp")) & (F.col("e.score") < 57.6), "left_anti")
)

result = (
    filtered
    .groupBy("student_id", "term")
    .pivot("grade_status", ["INCOMPLETE", "FAIL"])
    .agg(F.sum("enrolled_at_month"))
    .orderBy("student_id", "term")
)
