from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter(F.col("course_code").isNotNull())
    .select(F.lit("course").alias("src"), "level", "credits")
)

course_rows_2 = (
    df_courses
    .filter(F.col("dept").isin("CS", "Bio", "Art"))
    .select(F.lit("course_2").alias("src"), "dept", "credits")
)

stacked = course_rows.union(course_rows_2)

result = (
    stacked
    .groupBy("src")
    .agg(F.count("*").alias("n_rows"), F.sum("credits").alias("total_credits"))
    .orderBy("src")
)
