from pyspark.sql import functions as F

base = df_enrollments.filter((F.col("term") != "2025T2") & (F.col("course_id") <= 8))

enrollment_side = (
    base
    .filter(F.col("student_id") <= 10)
    .select("grade_status")
)

student_rows = df_students.select("cohort")

result = enrollment_side.intersect(student_rows)
