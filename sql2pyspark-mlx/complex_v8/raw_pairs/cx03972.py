from pyspark.sql import functions as F

prepared_students = (
    df_students
    .filter(F.col("enrolled_on") >= F.lit("2024-03-15").cast("date"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "is_international",
    )
)

student_rows = (
    df_students
    .filter(F.col("cohort").isin("C2024", "C2023", "C2022"))
    .select("id", "gpa")
)

enrollment_side = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.month(F.col("enrolled_at")).isin(1, 2)))
    .select(F.col("student_id").alias("id"), F.col("score").alias("gpa"))
)

result = (
    student_rows
    .unionByName(enrollment_side)
    .distinct()
    .orderBy("id")
)
