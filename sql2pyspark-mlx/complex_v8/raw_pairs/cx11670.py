from pyspark.sql import functions as F

cleaned = (
    df_courses
    .filter(F.col("level").isin("core", "advanced"))
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

course_side = cleaned.select("credits_filled", "credits")

enrollment_rows = (
    df_enrollments
    .select(
        F.col("id").alias("credits_filled"),
        F.col("student_id").alias("credits"),
    )
)

result = (
    course_side
    .unionByName(enrollment_rows)
    .orderBy("credits_filled")
)
