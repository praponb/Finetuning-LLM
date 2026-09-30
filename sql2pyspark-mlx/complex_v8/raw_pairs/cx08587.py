from pyspark.sql import functions as F

filtered_enrollments = (
    df_enrollments
    .filter((F.col("score") > 38.1) & (~F.col("grade_status").isin("FAIL", "WITHDRAWN")))
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

enrollment_side = (
    filtered_enrollments
    .filter(F.col("term").isNotNull())
    .select("student_id", F.col("term").alias("name"))
)

student_side = (
    df_students
    .filter(F.col("is_international"))
    .select(F.col("id").alias("student_id"), F.col("cohort").alias("name"))
)

result = (
    enrollment_side
    .union(student_side)
    .orderBy(F.col("student_id").desc(), "name")
)
