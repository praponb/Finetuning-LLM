from pyspark.sql import functions as F

course_side = (
    df_courses
    .select(
        F.col("credits").alias("ref_id"),
        "id",
        F.lit("course").alias("record_type"),
    )
)

course_rows = (
    df_courses
    .filter(F.col("course_code").isNotNull())
    .select("credits", "id", F.lit("course_2").alias("record_type"))
)

result = course_side.union(course_rows)
