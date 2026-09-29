from pyspark.sql import functions as F

recent = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-03-15").cast("date"))
    .select(
        "id",
        "student_name",
        "cohort",
        "gpa",
    )
)

student_rows = (
    df_students
    .filter(F.col("gpa") < 3.4)
    .select("id", "enrolled_on", F.lit("student").alias("record_type"))
)

student_side = (
    df_students
    .filter((F.col("gpa") > 2.5) & (F.col("major") == "Bio"))
    .select("id", "enrolled_on", F.lit("student_2").alias("record_type"))
)

student_side_2 = (
    df_students
    .filter((F.col("major") == "Art") & (F.col("enrolled_on") >= F.lit("2024-03-15").cast("date")))
    .select("id", "enrolled_on", F.lit("student_3").alias("record_type"))
)

result = (
    student_rows
    .union(student_side)
    .union(student_side_2)
    .orderBy("record_type", "enrolled_on")
)
