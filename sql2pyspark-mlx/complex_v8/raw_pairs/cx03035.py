from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("score") > 13.9)
    .select("course_id", "term")
)

student_side = (
    df_students
    .filter(F.col("major").isNotNull())
    .select("id", "student_name")
)

result = enrollment_side.subtract(student_side)
