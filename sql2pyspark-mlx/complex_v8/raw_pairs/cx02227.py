from pyspark.sql import functions as F

course_rows = df_courses.select(F.col("course_code").alias("category"))

enrollment_side = df_enrollments.select(F.col("term").alias("category"))

result = course_rows.subtract(enrollment_side)
