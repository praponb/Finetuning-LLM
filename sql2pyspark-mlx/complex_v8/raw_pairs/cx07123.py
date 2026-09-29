from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("score") > 42.4)
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

enrollment_side = recent_enrollments.select("course_id", F.col("student_id").alias("key_id"), "score")

enrollment_side_2 = df_enrollments.select("student_id", "course_id", "score")

result = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
    .orderBy(F.col("score").desc(), "course_id")
)
