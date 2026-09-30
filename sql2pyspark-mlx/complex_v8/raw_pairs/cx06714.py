from pyspark.sql import functions as F

base_students = (
    df_students
    .filter((~F.col("is_international")) & (F.col("cohort") != "C2021"))
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "gpa",
    )
)

student_rows = (
    base_students
    .filter(F.col("enrolled_on").between(F.lit("2024-04-04").cast("date"), F.lit("2025-08-27").cast("date")))
    .select(F.col("student_name").alias("category"), "gpa")
)

enrollment_side = df_enrollments.select(F.col("term").alias("category"), F.col("score").alias("gpa"))

combined = (
    student_rows
    .unionByName(enrollment_side)
    .distinct()
)

result = (
    combined
    .groupBy("category")
    .agg(F.count("*").alias("n_rows"), F.sum("gpa").alias("total_gpa"))
    .orderBy("category")
)
