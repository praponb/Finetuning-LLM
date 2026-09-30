from pyspark.sql import functions as F

prepared_courses = (
    df_courses
    .filter((F.col("credits") <= 4) & (F.col("dept").isin("Bio", "Biz", "Art")))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

course_rows = (
    prepared_courses
    .filter((F.col("level") == "advanced") & (F.col("credits") < 3))
    .select(F.lit("course").alias("origin"), F.col("credits").alias("key_id"), "id")
)

course_rows_2 = (
    df_courses
    .filter((F.col("dept").isNotNull()) & (F.col("level") == "intro"))
    .select(
        F.lit("course_2").alias("origin"),
        F.col("id").alias("key_id"),
        F.col("credits").alias("id"),
    )
)

result = (
    course_rows
    .unionByName(course_rows_2)
    .orderBy("origin")
)
