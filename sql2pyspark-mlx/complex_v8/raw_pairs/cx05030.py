from pyspark.sql import functions as F

student_side = (
    df_students
    .select(
        F.col("id").alias("qty"),
        F.col("enrolled_on").alias("day"),
        "gpa",
        F.lit("student").alias("src"),
    )
)

student_rows = (
    df_students
    .select(
        F.col("id").alias("qty"),
        F.col("enrolled_on").alias("day"),
        "gpa",
        F.lit("student_2").alias("src"),
    )
)

student_rows_2 = (
    df_students
    .select(
        F.col("id").alias("qty"),
        F.col("enrolled_on").alias("day"),
        "gpa",
        F.lit("student_3").alias("src"),
    )
)

result = (
    student_side
    .union(student_rows)
    .union(student_rows_2)
    .orderBy(F.col("qty").desc())
)
