from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter(F.col("grade_status") == "FAIL")
    .select(
        F.col("student_id").alias("entity_id"),
        F.col("grade_status").alias("label"),
    )
)

student_rows = (
    df_students
    .filter(F.col("gpa") > 2.7)
    .select(F.col("id").alias("entity_id"), F.col("student_name").alias("label"))
)

enrollment_rows_2 = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.hour(F.col("enrolled_at")).between(6, 10)))
    .select(F.col("id").alias("entity_id"), F.col("term").alias("label"))
)

result = (
    enrollment_rows
    .unionByName(student_rows)
    .unionByName(enrollment_rows_2)
    .orderBy(F.col("label").desc(), "entity_id")
)
