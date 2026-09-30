from pyspark.sql import functions as F

valid = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

course_rows = valid.select(F.col("id").alias("qty"))

student_side = (
    df_students
    .filter((F.year(F.col("enrolled_on")) == 2025) & (F.length(F.trim(F.col("student_name"))) >= 5))
    .select(F.col("id").alias("qty"))
)

result = (
    course_rows
    .intersectAll(student_side)
    .orderBy(F.col("qty").desc())
)
