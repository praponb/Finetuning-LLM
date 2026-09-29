from pyspark.sql import functions as F

valid = (
    df_courses
    .filter(F.col("level") == "advanced")
    .select(
        "id",
        "dept",
        "credits",
        "level",
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
)

course_rows = (
    valid
    .filter((F.col("course_code_slug").isNotNull()) & (F.col("dept") == "Bio"))
    .select(
        "credits",
        "course_code_slug",
        F.col("id").alias("ref_id"),
        F.lit("course").alias("origin"),
    )
)

course_side = (
    df_courses
    .select(
        F.col("id").alias("credits"),
        F.col("level").alias("course_code_slug"),
        F.col("credits").alias("ref_id"),
        F.lit("course_2").alias("origin"),
    )
)

result = (
    course_rows
    .unionByName(course_side)
    .orderBy("credits")
)
