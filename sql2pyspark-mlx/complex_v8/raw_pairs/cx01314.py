from pyspark.sql import functions as F

scoped = (
    df_students
    .filter((F.col("gpa") <= 3.4) & (F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

student_rows = scoped.select("id", "cohort")

student_side = (
    df_students
    .filter((~F.col("cohort").isin("C2024", "C2021")) & (F.col("gpa") < 3.4))
    .select("id", "student_name")
)

student_rows_2 = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 4)
    .select("id", "cohort")
)

result = (
    student_rows
    .union(student_side)
    .union(student_rows_2)
    .orderBy(F.col("cohort").desc())
)
