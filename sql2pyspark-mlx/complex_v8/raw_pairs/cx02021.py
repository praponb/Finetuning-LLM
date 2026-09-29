from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter((F.col("dept").isin("Bio", "CS")) & (F.col("level").isin("core", "advanced")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

course_side = prepared.select("id")

course_side_2 = df_courses.select(F.col("credits").alias("id"))

result = (
    course_side
    .subtract(course_side_2)
    .orderBy("id")
)
