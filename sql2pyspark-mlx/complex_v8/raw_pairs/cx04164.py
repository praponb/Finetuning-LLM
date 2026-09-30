from pyspark.sql import functions as F

course_rows = df_courses.select("id", "credits", F.lit("course").alias("record_type"))

course_side = df_courses.select("credits", "id", F.lit("course_2").alias("record_type"))

result = (
    course_rows
    .union(course_side)
    .distinct()
)
