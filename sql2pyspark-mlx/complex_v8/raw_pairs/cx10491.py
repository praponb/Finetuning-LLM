from pyspark.sql import functions as F

cleaned_students = (
    df_students
    .filter((F.lower(F.col("student_name")).like("%iz%")) & (F.col("cohort") != "C2023"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("cohort")), " ", "_")).alias("cohort_slug"),
    )
)

student_side = cleaned_students.select("id")

enrollment_side = df_enrollments.select(F.col("course_id").alias("id"))

result = (
    student_side
    .exceptAll(enrollment_side)
    .orderBy(F.col("id").desc())
)
