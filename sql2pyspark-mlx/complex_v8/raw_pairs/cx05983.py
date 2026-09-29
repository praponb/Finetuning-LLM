from pyspark.sql import functions as F

student_rows = (
    df_students
    .filter((F.col("enrolled_on") >= F.lit("2024-10-09").cast("date")) & (F.col("major") != "Math"))
    .select(
        F.lit("student").alias("src"),
        F.col("id").alias("key_id"),
        "cohort",
        F.col("enrolled_on").alias("event_date"),
    )
)

student_side = (
    df_students
    .select(
        F.lit("student_2").alias("src"),
        F.col("id").alias("key_id"),
        "cohort",
        F.col("enrolled_on").alias("event_date"),
    )
)

result = (
    student_rows
    .union(student_side)
    .distinct()
)
