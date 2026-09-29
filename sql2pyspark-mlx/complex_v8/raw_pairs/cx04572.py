from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("cohort") == "C2022") & (~F.col("is_international")))
    .select(
        "student_name",
        F.col("cohort").alias("label"),
        F.col("enrolled_on").alias("day"),
    )
)

student_rows_2 = (
    df_students
    .filter((F.col("gpa") > 2.1) & (F.col("cohort") == "C2023"))
    .select("cohort", "major", "enrolled_on")
)

result = (
    student_rows
    .union(student_rows_2)
    .distinct()
    .orderBy("student_name", "label")
)
