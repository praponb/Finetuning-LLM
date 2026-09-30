from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-06").cast("date")) & (F.col("grade_status").isin("FAIL", "WITHDRAWN")))
    .select(F.lit("enrollment").alias("src"), "id", "student_id")
)

enrollment_rows_2 = df_enrollments.select(F.lit("enrollment_2").alias("src"), "id", "course_id")

course_side = df_courses.select(F.lit("course").alias("src"), "credits", "id")

result = (
    enrollment_rows
    .union(enrollment_rows_2)
    .union(course_side)
)
