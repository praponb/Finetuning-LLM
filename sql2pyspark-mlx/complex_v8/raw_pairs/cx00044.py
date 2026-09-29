from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.col("credits") >= 2)
    .select(F.col("id").alias("qty"))
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") & (F.col("term") == "2025T1"))
    .select(F.col("id").alias("qty"))
)

result = (
    course_rows
    .subtract(enrollment_rows)
    .orderBy("qty")
)
