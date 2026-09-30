from pyspark.sql import functions as F

cleaned_courses = (
    df_courses
    .filter(F.col("level").isin("core", "advanced"))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
    )
)

course_side = cleaned_courses.select(F.col("id").alias("ref_id"), "credits")

enrollment_side = (
    df_enrollments
    .select(F.col("id").alias("ref_id"), F.col("course_id").alias("credits"))
)

stacked = (
    course_side
    .unionByName(enrollment_side)
    .distinct()
)

result = (
    stacked
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.sum("credits").alias("total_credits"))
    .orderBy("ref_id")
)
