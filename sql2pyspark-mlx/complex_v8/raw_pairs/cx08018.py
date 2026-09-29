from pyspark.sql import functions as F

cleaned = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.substring(F.trim(F.col("level")), 1, 2).alias("level_prefix"),
    )
)

course_rows = (
    cleaned
    .select(
        F.col("id").alias("qty"),
        F.col("credits").alias("entity_id"),
        "level_prefix",
    )
)

course_rows_2 = (
    df_courses
    .filter((F.lower(F.col("course_code")).like("%a1%")) & (F.col("level") == "advanced"))
    .select("id", "credits", "level")
)

result = (
    course_rows
    .union(course_rows_2)
    .distinct()
)
