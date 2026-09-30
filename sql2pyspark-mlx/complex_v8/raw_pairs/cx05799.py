from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter(F.col("level").isin("advanced", "core"))
    .select(F.col("course_code").alias("label"), F.col("dept").alias("name"))
)

course_rows = (
    df_courses
    .filter((F.col("credits") <= 4) & (F.upper(F.trim(F.col("course_code"))).like("C%")))
    .select("course_code", "level")
)

result = course_side.union(course_rows)
