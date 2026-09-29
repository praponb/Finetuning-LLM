from pyspark.sql import functions as F

recent_courses = (
    df_courses
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

course_side = (
    recent_courses
    .filter(F.col("credits") <= 3)
    .select(F.col("course_code").alias("name"))
)

student_side = df_students.select("student_name")

result = (
    course_side
    .subtract(student_side)
    .orderBy(F.col("name").desc())
)
