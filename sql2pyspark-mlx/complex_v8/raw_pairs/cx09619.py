from pyspark.sql import functions as F

prepared_students = (
    df_students
    .filter((F.col("gpa") >= 2.7) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.sum("score").alias("total_enrollment_score"),
        F.count(F.when(F.col("grade_status") == "PASS", 1)).alias("enrollment_grade_status_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

with_stats = (
    prepared_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.total_enrollment_score",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.min_enrollment_score",
    )
)

tagged = (
    with_stats
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        "total_enrollment_score",
        "enrollment_grade_status_count",
        F.concat_ws(": ", F.col("major"), F.col("student_name")).alias("major_student_name_label"),
        F.greatest(F.col("gpa"), F.col("min_enrollment_score")).alias("greatest_gpa_min_enrollment_score"),
    )
)

result = (
    tagged
    .select("greatest_gpa_min_enrollment_score")
    .distinct()
    .orderBy("greatest_gpa_min_enrollment_score")
)
