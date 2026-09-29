from pyspark.sql import functions as F

recent = (
    df_courses
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.lower(F.regexp_replace(F.trim(F.col("course_code")), " ", "_")).alias("course_code_slug"),
    )
)

course_rows = (
    recent
    .filter((F.col("dept") == "Bio") & (F.col("course_code").isNotNull()))
    .select(F.col("course_code").alias("name"))
)

student_rows = (
    df_students
    .filter(F.col("is_international"))
    .select(F.col("cohort").alias("name"))
)

result = course_rows.subtract(student_rows)
