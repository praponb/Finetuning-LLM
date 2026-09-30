from pyspark.sql import functions as F

scoped = (
    df_students
    .filter(F.col("enrolled_on").isNotNull())
    .select(
        "id",
        "student_name",
        "major",
        "gpa",
        "is_international",
    )
)

student_rows = scoped.select("major")

course_rows = (
    df_courses
    .filter(F.col("dept") == "Math")
    .select(F.col("dept").alias("major"))
)

result = (
    student_rows
    .intersect(course_rows)
    .orderBy("major")
)
