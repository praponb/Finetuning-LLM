from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter(F.col("level") != "advanced")
    .select(
        F.col("credits").alias("key_id"),
        F.col("id").alias("ref_id"),
        F.lit("course").alias("source"),
    )
)

course_rows = (
    df_courses
    .select(
        F.col("id").alias("key_id"),
        F.col("credits").alias("ref_id"),
        F.lit("course_2").alias("source"),
    )
)

result = (
    course_side
    .unionByName(course_rows)
    .orderBy("source")
)
