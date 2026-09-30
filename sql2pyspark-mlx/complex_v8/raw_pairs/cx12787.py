from pyspark.sql import functions as F

scoped_courses = (
    df_courses
    .filter(F.col("level") == "core")
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.when(F.col("credits") >= 3, "HIGH").when(F.col("credits") >= 2, "MEDIUM").otherwise("LOW").alias("credits_tier"),
    )
)

course_rows = (
    scoped_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 5) & (F.col("level") != "core"))
    .select(
        "id",
        "credits_tier",
        "credits",
        F.lit("course").alias("record_type"),
    )
)

course_rows_2 = (
    df_courses
    .filter(F.col("course_code").isNotNull())
    .select(
        "credits",
        "level",
        "id",
        F.lit("course_2").alias("record_type"),
    )
)

result = (
    course_rows
    .union(course_rows_2)
    .orderBy(F.col("credits").desc())
)
