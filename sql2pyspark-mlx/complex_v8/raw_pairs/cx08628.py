from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("id", "enrolled_at")

enrollment_side_2 = df_enrollments.select("course_id", "enrolled_at")

enrollment_side_3 = df_enrollments.select("id", "enrolled_at")

result = (
    enrollment_side
    .union(enrollment_side_2)
    .union(enrollment_side_3)
)
