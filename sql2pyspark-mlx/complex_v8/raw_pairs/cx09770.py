from pyspark.sql import functions as F

recent = (
    df_courses
    .filter((F.length(F.trim(F.col("course_code"))) >= 5) & (F.col("level").isin("intro", "core")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

course_rows = recent.select("credits", F.col("dept").alias("category"))

student_side = df_students.select("id", "major")

result = (
    course_rows
    .intersectAll(student_side)
    .orderBy("category", "credits")
)
