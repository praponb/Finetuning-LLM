from pyspark.sql import functions as F

scoped = (
    df_students
    .filter(F.col("cohort").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
    )
)

student_side = (
    scoped
    .filter(F.lower(F.col("student_name")).like("%na%"))
    .select(
        F.lit("student").alias("src"),
        F.col("student_name").alias("category"),
        "enrolled_on",
    )
)

student_rows = (
    df_students
    .filter(F.col("cohort").isin("C2024", "C2022"))
    .select(
        F.lit("student_2").alias("src"),
        F.col("major").alias("category"),
        "enrolled_on",
    )
)

stacked = student_side.unionByName(student_rows)

result = (
    stacked
    .groupBy("src")
    .agg(
        F.count("*").alias("n_rows"),
        F.max("enrolled_on").alias("latest_enrolled_on"),
    )
    .orderBy("src")
)
