from pyspark.sql import functions as F

course_rows = df_courses.select("id", F.col("credits").alias("key_id"))

course_side = df_courses.select("id", F.col("credits").alias("key_id"))

unioned = course_rows.unionByName(course_side)

result = (
    unioned
    .groupBy("id")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("id")
)
