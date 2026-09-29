from pyspark.sql import functions as F

course_side = df_courses.select(F.col("id").alias("entity_id"), "dept")

enrollment_side = df_enrollments.select("id", "grade_status")

result = course_side.intersect(enrollment_side)
