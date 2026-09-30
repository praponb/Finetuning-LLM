from pyspark.sql import functions as F

student_side = (
    df_students
    .select(
        F.lit("student").alias("source"),
        F.col("id").alias("entity_id"),
        "enrolled_on",
    )
)

student_side_2 = (
    df_students
    .filter((F.col("gpa") > 2.0) & (F.col("major") == "Art"))
    .select(
        F.lit("student_2").alias("source"),
        F.col("id").alias("entity_id"),
        "enrolled_on",
    )
)

result = (
    student_side
    .unionByName(student_side_2)
    .orderBy(F.col("enrolled_on").desc(), "source")
)
