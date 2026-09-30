from pyspark.sql import functions as F

enrollment_rows = df_enrollments.select(F.col("term").alias("label"))

enrollment_side = df_enrollments.select("grade_status")

result = enrollment_rows.intersect(enrollment_side)
