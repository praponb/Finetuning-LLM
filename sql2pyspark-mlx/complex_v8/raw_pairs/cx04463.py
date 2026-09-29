from pyspark.sql import functions as F

course_rows = df_courses.select("id")

enrollment_side = (
    df_enrollments
    .filter((F.col("score").between(8.7, 79.2)) & (F.col("enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")))
    .select("id")
)

result = (
    course_rows
    .subtract(enrollment_side)
    .orderBy(F.col("id").desc())
)
