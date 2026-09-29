from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") < 57.0) & (F.col("course_id") <= 2))
    .select(F.col("student_id").alias("key_id"))
)

enrollment_side = df_enrollments.select(F.col("course_id").alias("key_id"))

result = (
    enrollment_rows
    .subtract(enrollment_side)
    .orderBy("key_id")
)
