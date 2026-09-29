from pyspark.sql import functions as F

student_side = (
    df_students
    .filter(~F.col("is_international"))
    .select("cohort", "id", "major")
)

student_side_2 = df_students.select("student_name", "id", "cohort")

result = (
    student_side
    .union(student_side_2)
    .orderBy("id")
)
