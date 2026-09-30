from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-24 12:00:00").cast("timestamp")) & (F.col("term").isin("2024T1", "2025T2", "2024T2")))
    .select("term")
)

course_side = (
    df_courses
    .filter(F.col("dept").isin("Bio", "Math"))
    .select(F.col("course_code").alias("term"))
)

result = (
    enrollment_side
    .subtract(course_side)
    .orderBy("term")
)
