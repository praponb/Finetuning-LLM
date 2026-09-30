from pyspark.sql import functions as F

scoped = (
    df_students
    .filter((F.col("major") != "CS") & (F.year(F.col("enrolled_on")) == 2024))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "gpa",
        F.date_format(F.col("enrolled_on"), "MMM yyyy").alias("enrolled_on_label"),
    )
)

student_side = (
    scoped
    .filter((F.col("cohort") == "C2024") & (F.col("enrolled_on_label").isNotNull()))
    .select("id")
)

course_side = df_courses.select("id")

result = (
    student_side
    .exceptAll(course_side)
    .orderBy(F.col("id").desc())
)
