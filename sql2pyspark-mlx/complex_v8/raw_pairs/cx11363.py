from pyspark.sql import functions as F

base_courses = (
    df_courses
    .filter((F.col("credits") < 3) & (F.col("level") == "advanced"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.length(F.trim(F.col("dept"))).alias("dept_len"),
    )
)

course_rows = base_courses.select(F.col("course_code").alias("label"))

course_side = (
    df_courses
    .filter(F.col("credits") >= 2)
    .select("course_code")
)

result = (
    course_rows
    .subtract(course_side)
    .orderBy(F.col("label").desc())
)
