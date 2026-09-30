from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter(F.col("score") > 13.9)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.length(F.trim(F.col("term"))).alias("term_len"),
    )
)

enrollment_side = (
    valid_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "student_id",
        F.col("term").alias("label"),
        F.lit("enrollment").alias("origin"),
    )
)

student_side = (
    df_students
    .select(
        F.col("id").alias("student_id"),
        F.col("major").alias("label"),
        F.lit("student").alias("origin"),
    )
)

course_side = (
    df_courses
    .filter((F.col("level").isin("advanced", "intro")) & (F.lower(F.col("course_code")).like("%01%")))
    .select(
        F.col("credits").alias("student_id"),
        F.col("course_code").alias("label"),
        F.lit("course").alias("origin"),
    )
)

result = (
    enrollment_side
    .unionByName(student_side)
    .unionByName(course_side)
    .orderBy(F.col("origin").desc())
)
