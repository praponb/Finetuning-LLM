from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("score") <= 59.8) & (F.col("grade_status") == "INCOMPLETE"))
    .select(F.col("student_id").alias("qty"))
)

course_side = df_courses.select(F.col("credits").alias("qty"))

result = enrollment_side.intersect(course_side)
