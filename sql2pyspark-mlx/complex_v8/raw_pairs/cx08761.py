from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("gpa") >= 2.0) & (F.col("is_international")))
    .select(
        F.col("major").alias("category"),
        F.col("enrolled_on").alias("event_date"),
    )
)

student_side_2 = df_students.select("major", "enrolled_on")

student_side_3 = (
    df_students
    .filter(F.col("cohort") == "C2021")
    .select("cohort", "enrolled_on")
)

result = (
    student_side
    .union(student_side_2)
    .union(student_side_3)
    .orderBy(F.col("event_date").desc(), "category")
)
