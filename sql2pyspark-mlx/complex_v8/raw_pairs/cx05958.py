from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter((F.col("student_id") >= 17) & (F.month(F.col("enrolled_at")).isin(2, 3)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat_ws(" / ", F.col("grade_status"), F.col("term")).alias("grade_status_term_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count(F.when(F.col("grade_status") != "PASS", 1)).alias("enrollment_grade_status_count"),
    )
)

with_counts = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
    )
)

result = (
    with_counts
    .groupBy("enrollment_count", "student_id")
    .pivot("grade_status", ["PASS", "INCOMPLETE", "FAIL"])
    .agg(F.min("score"))
)
