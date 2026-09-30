from pyspark.sql import functions as F

prepared = (
    df_students
    .filter((F.col("enrolled_on").isNotNull()) & (~F.col("is_international")))
    .select(
        "id",
        "major",
        "gpa",
        "is_international",
    )
)

student_rows = (
    df_students
    .filter(F.length(F.trim(F.col("student_name"))) >= 5)
    .select(
        "id",
        "enrolled_on",
        F.col("gpa").alias("metric"),
        F.lit("student").alias("source"),
    )
)

student_rows_2 = (
    df_students
    .select(
        "id",
        "enrolled_on",
        "gpa",
        F.lit("student_2").alias("source"),
    )
)

result = (
    student_rows
    .union(student_rows_2)
    .distinct()
    .orderBy("enrolled_on")
)
