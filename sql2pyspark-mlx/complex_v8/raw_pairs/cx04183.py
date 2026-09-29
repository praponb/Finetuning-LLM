from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("L%"))
    .select(F.col("id").alias("key_id"), F.col("cohort").alias("label"))
)

enrollment_rows = (
    df_enrollments
    .select(F.col("student_id").alias("key_id"), F.col("term").alias("label"))
)

result = (
    student_side
    .intersect(enrollment_rows)
    .orderBy("label")
)
