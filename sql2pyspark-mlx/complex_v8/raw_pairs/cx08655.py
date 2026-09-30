from pyspark.sql import functions as F

student_side = (
    df_students
    .filter((F.col("enrolled_on").between(F.lit("2024-01-24").cast("date"), F.lit("2025-07-03").cast("date"))) & (F.col("gpa") <= 3.2))
    .select(
        F.col("id").alias("entity_id"),
        "enrolled_on",
        F.col("student_name").alias("label"),
        F.lit("student").alias("src"),
    )
)

student_rows = (
    df_students
    .filter((F.col("cohort") == "C2022") & (F.col("gpa") >= 2.7))
    .select(
        F.col("id").alias("entity_id"),
        "enrolled_on",
        F.col("cohort").alias("label"),
        F.lit("student_2").alias("src"),
    )
)

result = (
    student_side
    .unionByName(student_rows)
    .distinct()
    .orderBy(F.col("entity_id").desc())
)
