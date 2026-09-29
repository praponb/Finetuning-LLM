from pyspark.sql import functions as F

recent = (
    df_students
    .filter((F.col("gpa") < 3.2) & (~F.col("is_international")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "gpa",
        "is_international",
        F.concat_ws(" / ", F.col("cohort"), F.col("student_name")).alias("cohort_student_name_label"),
    )
)

student_side = (
    recent
    .select(
        F.lit("student").alias("src"),
        F.col("id").alias("key_id"),
        "cohort_student_name_label",
    )
)

student_rows = (
    df_students
    .filter(F.col("student_name").isNotNull())
    .select(F.lit("student_2").alias("src"), "id", "major")
)

course_side = df_courses.select(F.lit("course").alias("src"), "id", "level")

result = (
    student_side
    .union(student_rows)
    .union(course_side)
    .orderBy(F.col("key_id").desc())
)
