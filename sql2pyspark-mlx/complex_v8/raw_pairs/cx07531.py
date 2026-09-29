from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.col("credits") > 2)
    .select("level")
)

course_side = (
    df_courses
    .filter(F.col("dept") == "Art")
    .select(F.col("dept").alias("level"))
)

result = (
    course_rows
    .subtract(course_side)
    .orderBy(F.col("level").desc())
)
