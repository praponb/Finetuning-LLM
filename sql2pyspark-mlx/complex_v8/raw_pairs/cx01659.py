from pyspark.sql import functions as F

scoped = (
    df_courses
    .filter((F.col("credits") <= 3) & (F.col("dept").isin("CS", "Math")))
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        F.concat(F.col("dept"), F.lit("#"), F.col("id").cast("string")).alias("dept_ref"),
    )
)

course_side = scoped.select(F.col("id").alias("entity_id"), F.col("credits").alias("key_id"))

enrollment_rows = (
    df_enrollments
    .filter((F.col("student_id") >= 14) & (F.col("score") > 36.6))
    .select("course_id", "student_id")
)

result = (
    course_side
    .union(enrollment_rows)
    .orderBy(F.col("key_id").desc())
)
