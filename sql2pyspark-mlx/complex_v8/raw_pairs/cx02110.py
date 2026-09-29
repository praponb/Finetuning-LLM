from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(F.col("course_id").alias("entity_id"), F.col("term").alias("label"))
)

student_rows = df_students.select("id", "cohort")

result = (
    enrollment_side
    .union(student_rows)
    .distinct()
)
