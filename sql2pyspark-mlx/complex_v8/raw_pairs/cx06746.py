from pyspark.sql import functions as F

student_side = df_students.select("id")

student_side_2 = (
    df_students
    .filter((F.col("enrolled_on") < F.lit("2025-07-03").cast("date")) & (F.col("cohort") == "C2021"))
    .select("id")
)

result = (
    student_side
    .intersect(student_side_2)
    .orderBy("id")
)
