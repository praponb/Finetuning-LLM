from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter(F.col("dept").isin("Biz", "Art", "CS"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

course_side = prepared.select(F.col("course_code").alias("category"))

course_side_2 = df_courses.select("course_code")

result = (
    course_side
    .intersectAll(course_side_2)
    .orderBy(F.col("category").desc())
)
