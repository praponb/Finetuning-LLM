from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "INCOMPLETE"), "group_a").when(F.col("grade_status") == "FAIL", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enrollment_side = (
    filtered_enrollments
    .filter(F.col("student_id") >= 6)
    .select(F.col("grade_status_group").alias("label"))
)

enrollment_rows = (
    df_enrollments
    .filter(F.col("course_id") >= 9)
    .select("term")
)

result = enrollment_side.subtract(enrollment_rows)
