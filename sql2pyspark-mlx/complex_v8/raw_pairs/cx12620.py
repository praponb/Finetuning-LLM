from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.upper(F.trim(F.col("course_code"))).like("A%"))
    .select("course_code", "level")
)

course_side = (
    df_courses
    .filter(F.col("credits") >= 2)
    .select(F.col("dept").alias("course_code"), "level")
)

unioned = course_rows.unionByName(course_side)

result = (
    unioned
    .groupBy("course_code")
    .agg(F.count("*").alias("n_rows"))
    .orderBy("course_code")
)
