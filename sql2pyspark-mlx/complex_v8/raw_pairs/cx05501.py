from pyspark.sql import functions as F

course_side = df_courses.select("credits", "level")

enrollment_side = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-02-12 10:00:00").cast("timestamp")) & (F.col("score") <= 83.1))
    .select("course_id", "term")
)

result = course_side.intersect(enrollment_side)
