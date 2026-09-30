from pyspark.sql import functions as F

valid = (
    df_students
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("grade_status") == "FAIL", 1)).alias("enrollment_grade_status_count"),
        F.count(F.when(F.col("term") == "2024T1", 1)).alias("enrollment_term_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
    )
)

with_children = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.is_international",
        "s.major_or_unknown",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.course_id").alias("enrollment_course_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

result = (
    with_children
    .groupBy("enrollment_grade_status")
    .pivot("major_or_unknown", ["Bio", "Math", "CS", "Art"])
    .agg(F.sum("enrollment_score"))
)
