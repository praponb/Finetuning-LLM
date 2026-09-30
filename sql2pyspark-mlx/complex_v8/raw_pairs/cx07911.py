from pyspark.sql import functions as F

valid = (
    df_courses
    .filter((F.col("level") != "core") & (~F.col("dept").isin("Math", "Bio")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
    )
)

scored = (
    valid
    .select(
        "id",
        "course_code",
        F.lower(F.regexp_replace(F.trim(F.col("dept")), " ", "_")).alias("dept_slug"),
        F.concat_ws(" / ", F.col("dept"), F.col("course_code")).alias("dept_course_code_label"),
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

result = (
    scored
    .select("dept_course_code_label", "id", "course_code")
    .distinct()
    .orderBy(F.col("course_code").asc(), F.col("id").asc())
)
