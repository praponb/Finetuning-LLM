from pyspark.sql import functions as F

enrollment_side = df_enrollments.select(F.col("student_id").alias("qty"))

course_side = df_courses.select(F.col("credits").alias("qty"))

result = enrollment_side.subtract(course_side)
