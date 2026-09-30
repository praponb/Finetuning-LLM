from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select("course_id", "student_id")

course_side = df_courses.select(F.col("credits").alias("course_id"), F.col("id").alias("student_id"))

enrollment_rows_2 = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        F.col("student_id").alias("course_id"),
        F.col("course_id").alias("student_id"),
    )
)

result = (
    enrollment_rows
    .union(course_side)
    .union(enrollment_rows_2)
    .orderBy("student_id", "course_id")
)
