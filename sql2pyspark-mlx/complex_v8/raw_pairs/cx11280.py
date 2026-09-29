from pyspark.sql import functions as F

cleaned_students = (
    df_students
    .filter(F.col("is_international"))
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        F.when(F.col("gpa") >= 3.4, "HIGH").when(F.col("gpa") >= 2.7, "MEDIUM").otherwise("LOW").alias("gpa_tier"),
    )
)

student_side = (
    cleaned_students
    .select("student_name", F.col("major").alias("category"), "enrolled_on")
)

student_side_2 = df_students.select("cohort", "major", "enrolled_on")

stacked = student_side.union(student_side_2)

result = (
    stacked
    .groupBy("student_name")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("student_name")
)
