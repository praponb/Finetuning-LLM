from pyspark.sql import functions as F

scoped = (
    df_students
    .filter(~F.col("cohort").isin("C2022", "C2023"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("gpa") >= 3.3, "HIGH").when(F.col("gpa") >= 2.1, "MEDIUM").otherwise("LOW").alias("gpa_tier"),
    )
)

student_rows = (
    scoped
    .filter(F.col("gpa") <= 3.4)
    .select(F.col("gpa_tier").alias("category"), "gpa")
)

student_rows_2 = df_students.select("major", "gpa")

result = (
    student_rows
    .union(student_rows_2)
    .orderBy(F.col("gpa").desc(), "category")
)
