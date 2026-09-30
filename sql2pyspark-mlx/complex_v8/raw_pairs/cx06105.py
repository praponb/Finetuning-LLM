from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("J%"))
    .select("id", "major")
)

student_rows = df_students.select("id", F.col("student_name").alias("major"))

result = (
    student_side
    .subtract(student_rows)
    .orderBy(F.col("id").desc(), "major")
)
