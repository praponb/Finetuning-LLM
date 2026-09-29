from pyspark.sql import functions as F

filtered_students = (
    df_students
    .filter((F.col("is_international")) & (F.col("enrolled_on") < F.lit("2025-07-03").cast("date")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.upper(F.trim(F.col("cohort"))).alias("cohort_clean"),
    )
)

student_side = (
    filtered_students
    .filter(F.col("enrolled_on").between(F.lit("2024-02-28").cast("date"), F.lit("2025-08-27").cast("date")))
    .select("id", F.col("enrolled_on").alias("day"))
)

student_rows = (
    df_students
    .filter((F.col("gpa").between(2.7, 3.2)) & (~F.col("cohort").isin("C2021", "C2022")))
    .select("id", F.col("enrolled_on").alias("day"))
)

result = student_side.union(student_rows)
