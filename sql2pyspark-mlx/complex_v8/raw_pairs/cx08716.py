from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter((F.col("gpa") < 3.3) | (F.col("enrolled_on").between(F.lit("2024-02-28").cast("date"), F.lit("2025-08-27").cast("date"))))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("gpa") >= 3.2, "A").when(F.col("gpa") >= 2.7, "B").otherwise("C").alias("gpa_tier"),
    )
)

student_side = (
    valid_students
    .filter(F.upper(F.trim(F.col("student_name"))).like("I%"))
    .select(F.col("id").alias("key_id"), F.col("gpa").alias("value"))
)

student_rows = (
    df_students
    .filter((F.year(F.col("enrolled_on")) == 2024) & (F.col("student_name").isNotNull()))
    .select("id", "gpa")
)

result = student_side.union(student_rows)
