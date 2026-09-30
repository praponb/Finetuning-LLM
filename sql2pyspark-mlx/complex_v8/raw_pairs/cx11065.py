from pyspark.sql import functions as F

course_side = df_courses.select("id")

course_side_2 = df_courses.select("id")

result = course_side.intersect(course_side_2)
