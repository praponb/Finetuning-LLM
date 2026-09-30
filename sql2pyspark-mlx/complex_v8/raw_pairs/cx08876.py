from pyspark.sql import functions as F

scoped = (
    df_students
    .filter((F.year(F.col("enrolled_on")) == 2024) & (F.col("gpa") >= 2.7))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.lower(F.regexp_replace(F.trim(F.col("major")), " ", "_")).alias("major_slug"),
    )
)

student_rows = (
    scoped
    .filter(F.col("major").isin("Biz", "CS"))
    .select(
        "id",
        "gpa",
        "enrolled_on",
        F.lit("student").alias("origin"),
    )
)

student_side = (
    df_students
    .filter(F.col("enrolled_on").isNotNull())
    .select(
        "id",
        "gpa",
        "enrolled_on",
        F.lit("student_2").alias("origin"),
    )
)

result = (
    student_rows
    .union(student_side)
    .distinct()
    .orderBy(F.col("id").desc())
)
