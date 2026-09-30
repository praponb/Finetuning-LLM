from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("score").between(2.2, 59.8))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_rows = scoped.select(F.col("id").alias("key_id"), "enrolled_at", "course_id")

enrollment_side = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("course_id") >= 8))
    .select(
        F.col("course_id").alias("key_id"),
        "enrolled_at",
        F.col("student_id").alias("course_id"),
    )
)

result = (
    enrollment_rows
    .unionByName(enrollment_side)
    .orderBy(F.col("course_id").desc(), "enrolled_at")
)
