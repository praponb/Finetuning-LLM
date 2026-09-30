from pyspark.sql import functions as F

base = (
    df_courses
    .filter(F.col("credits") > 2)
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

course_side = (
    base
    .filter((F.col("level") == "intro") & (F.col("dept").isin("Art", "Math", "CS")))
    .select(F.col("id").alias("key_id"))
)

student_side = df_students.select("id")

result = (
    course_side
    .subtract(student_side)
    .orderBy("key_id")
)
