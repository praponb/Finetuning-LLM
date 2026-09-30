from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter((F.lower(F.col("course_code")).like("%s3%")) & (F.col("level") != "advanced"))
    .select("level")
)

course_side = df_courses.select("level")

result = (
    course_rows
    .intersect(course_side)
    .orderBy(F.col("level").desc())
)
