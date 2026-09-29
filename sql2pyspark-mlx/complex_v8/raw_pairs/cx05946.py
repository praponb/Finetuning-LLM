from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter((F.upper(F.trim(F.col("student_name"))).like("I%")) & (F.col("gpa") >= 2.7))
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

student_side = (
    valid_students
    .select(
        "cohort",
        "student_name",
        F.col("enrolled_on").alias("day"),
        F.lit("student").alias("source"),
    )
)

student_side_2 = (
    df_students
    .select(
        "major",
        "student_name",
        "enrolled_on",
        F.lit("student_2").alias("source"),
    )
)

result = student_side.union(student_side_2)
