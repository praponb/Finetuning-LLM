from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter((F.col("score") >= 38.1) & (F.col("term") == "2024T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.round(F.col("score") * 0.25, 2).alias("score_adj"),
    )
)

enrollment_side = (
    scoped
    .filter(F.col("grade_status") == "PASS")
    .select("course_id", "enrolled_at", F.lit("enrollment").alias("record_type"))
)

enrollment_rows = (
    df_enrollments
    .select(
        F.col("id").alias("course_id"),
        "enrolled_at",
        F.lit("enrollment_2").alias("record_type"),
    )
)

result = (
    enrollment_side
    .unionByName(enrollment_rows)
    .distinct()
    .orderBy("course_id")
)
