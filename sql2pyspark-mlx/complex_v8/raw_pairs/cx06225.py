from pyspark.sql import functions as F

base_students = (
    df_students
    .filter(F.col("major") == "Math")
    .select(
        "id",
        "student_name",
        "cohort",
        "gpa",
        "is_international",
        F.coalesce(F.col("cohort"), F.lit("UNKNOWN")).alias("cohort_or_unknown"),
    )
)

student_rows = df_students.select(F.col("major").alias("category"), F.col("enrolled_on").alias("day"))

student_rows_2 = (
    df_students
    .select(F.col("cohort").alias("category"), F.col("enrolled_on").alias("day"))
)

result = student_rows.unionByName(student_rows_2)
