from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter(F.col("gpa") < 3.4)
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.date_format(F.col("enrolled_on"), "MMM yyyy").alias("enrolled_on_label"),
    )
)

student_side = (
    valid_students
    .filter(F.col("cohort").isin("C2023", "C2021", "C2022"))
    .select(
        F.lit("student").alias("origin"),
        F.col("student_name").alias("category"),
        "cohort",
        "gpa",
    )
)

enrollment_side = (
    df_enrollments
    .select(
        F.lit("enrollment").alias("origin"),
        F.col("grade_status").alias("category"),
        F.col("term").alias("cohort"),
        F.col("score").alias("gpa"),
    )
)

enrollment_rows = (
    df_enrollments
    .select(
        F.lit("enrollment").alias("origin"),
        F.col("term").alias("category"),
        F.col("grade_status").alias("cohort"),
        F.col("score").alias("gpa"),
    )
)

stacked = (
    student_side
    .unionByName(enrollment_side)
    .unionByName(enrollment_rows)
)

result = (
    stacked
    .groupBy("origin")
    .agg(F.count("*").alias("n_rows"), F.sum("gpa").alias("total_gpa"))
    .orderBy("origin")
)
