from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("term").alias("label"),
        F.col("score").alias("value"),
        F.col("course_id").alias("ref_id"),
    )
)

student_side = (
    df_students
    .filter((F.col("major") == "Art") & (F.col("gpa") > 2.7))
    .select("cohort", "gpa", "id")
)

result = (
    enrollment_side
    .union(student_side)
    .distinct()
)
