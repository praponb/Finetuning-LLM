from pyspark.sql import functions as F

filtered = (
    df_enrollments
    .filter((F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE")) & (F.col("course_id") >= 3))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_side = (
    filtered
    .filter(F.col("term") == "2024T1")
    .select(
        F.lit("enrollment").alias("record_type"),
        F.col("student_id").alias("entity_id"),
        F.col("term").alias("name"),
    )
)

course_side = (
    df_courses
    .select(
        F.lit("course").alias("record_type"),
        F.col("credits").alias("entity_id"),
        F.col("level").alias("name"),
    )
)

result = (
    enrollment_side
    .unionByName(course_side)
    .orderBy(F.col("name").desc())
)
