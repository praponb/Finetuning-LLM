from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("id").alias("key_id"),
        F.col("student_id").alias("key_id_2"),
        F.col("grade_status").alias("label"),
        F.lit("enrollment").alias("record_type"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") >= 30.0) & (F.col("enrolled_at") < F.lit("2025-03-02 09:00:00").cast("timestamp")))
    .select(
        F.col("student_id").alias("key_id"),
        F.col("course_id").alias("key_id_2"),
        F.col("grade_status").alias("label"),
        F.lit("enrollment_2").alias("record_type"),
    )
)

course_side = (
    df_courses
    .filter(F.col("credits") <= 4)
    .select(
        F.col("id").alias("key_id"),
        F.col("credits").alias("key_id_2"),
        F.col("level").alias("label"),
        F.lit("course").alias("record_type"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .union(course_side)
    .orderBy(F.col("label").desc(), "record_type")
)
