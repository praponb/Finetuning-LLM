from pyspark.sql import functions as F

recent = (
    df_courses
    .filter(F.lower(F.col("course_code")).like("%01%"))
    .select(
        "id",
        "course_code",
        "dept",
        "level",
    )
)

course_side = (
    recent
    .filter(F.col("level") == "advanced")
    .select("id")
)

course_side_2 = (
    df_courses
    .filter((F.upper(F.trim(F.col("course_code"))).like("M%")) & (F.col("dept") == "Bio"))
    .select("id")
)

result = course_side.exceptAll(course_side_2)
