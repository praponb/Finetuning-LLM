from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter((F.col("credits") > 2) & (F.col("dept") == "Bio"))
    .select(F.lit("course").alias("source"), "course_code", "credits")
)

course_rows_2 = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("dept") == "Biz"))
    .select(F.lit("course_2").alias("source"), "dept", "id")
)

unioned = course_rows.union(course_rows_2)

result = (
    unioned
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("credits").alias("total_credits"))
    .orderBy("source")
)
