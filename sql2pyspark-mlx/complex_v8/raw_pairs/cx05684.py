from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter((F.col("level") == "intro") & (F.col("credits") > 2))
    .select("id")
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("course_id").isNotNull()) & (F.col("score") <= 87.8))
    .select("id")
)

result = (
    course_rows
    .intersectAll(enrollment_rows)
    .orderBy("id")
)
