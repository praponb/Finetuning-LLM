from pyspark.sql import functions as F

prepared = (
    df_students
    .filter(F.col("gpa").between(2.0, 3.4))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
    )
)

student_side = (
    prepared
    .filter(F.year(F.col("enrolled_on")) == 2025)
    .select(
        F.col("cohort").alias("name"),
        F.col("major").alias("name_2"),
        F.lit("student").alias("src"),
    )
)

course_rows = (
    df_courses
    .filter((F.col("credits") >= 2) & (F.col("dept").isin("CS", "Math", "Art")))
    .select(
        F.col("course_code").alias("name"),
        F.col("dept").alias("name_2"),
        F.lit("course").alias("src"),
    )
)

result = student_side.unionByName(course_rows)
