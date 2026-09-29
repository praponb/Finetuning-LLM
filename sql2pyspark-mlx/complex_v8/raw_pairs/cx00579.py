from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
)

enrollment_side = (
    scoped
    .filter(F.col("term").isNotNull())
    .select(
        F.col("course_id").alias("ref_id"),
        "score_filled",
        F.lit("enrollment").alias("source"),
    )
)

student_side = (
    df_students
    .filter(F.col("gpa") <= 3.4)
    .select("id", "gpa", F.lit("student").alias("source"))
)

student_rows = df_students.select("id", "gpa", F.lit("student").alias("source"))

result = (
    enrollment_side
    .union(student_side)
    .union(student_rows)
    .orderBy("source", "ref_id")
)
