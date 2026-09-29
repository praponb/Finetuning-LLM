from pyspark.sql import functions as F

course_side = df_courses.select("credits", "dept")

course_side_2 = (
    df_courses
    .filter(F.col("level").isNotNull())
    .select("id", "dept")
)

result = (
    course_side
    .intersect(course_side_2)
    .orderBy("dept")
)
