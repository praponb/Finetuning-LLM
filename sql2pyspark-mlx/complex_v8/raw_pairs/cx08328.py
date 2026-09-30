from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter(F.col("dept") != "Math")
    .select(
        F.col("credits").alias("ref_id"),
        F.col("id").alias("entity_id"),
        F.lit("course").alias("record_type"),
    )
)

enrollment_side = df_enrollments.select("id", "student_id", F.lit("enrollment").alias("record_type"))

result = (
    course_side
    .union(enrollment_side)
    .distinct()
    .orderBy(F.col("record_type").desc())
)
