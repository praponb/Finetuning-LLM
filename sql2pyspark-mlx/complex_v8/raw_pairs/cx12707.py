from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .filter((F.col("term") == "2024T1") & (F.col("course_id") >= 5))
    .select(F.col("id").alias("entity_id"))
)

course_side = df_courses.select("id")

result = (
    enrollment_rows
    .exceptAll(course_side)
    .orderBy(F.col("entity_id").desc())
)
