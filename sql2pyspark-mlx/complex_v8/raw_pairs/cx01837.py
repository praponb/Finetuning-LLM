from pyspark.sql import functions as F

enrollment_side = df_enrollments.select("student_id")

student_side = (
    df_students
    .filter((~F.col("is_international")) & (F.col("major") == "Bio"))
    .select("id")
)

result = (
    enrollment_side
    .subtract(student_side)
    .orderBy(F.col("student_id").desc())
)
