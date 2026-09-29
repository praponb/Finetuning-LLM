from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("grade_status") == "INCOMPLETE") & (F.col("term") == "2025T2"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

enrollment_rows = (
    valid_enrollments
    .select("id", F.col("grade_status").alias("category"), "enrolled_at")
)

enrollment_side = df_enrollments.select("course_id", "term", "enrolled_at")

unioned = (
    enrollment_rows
    .union(enrollment_side)
    .distinct()
)

result = (
    unioned
    .groupBy("category")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("id").alias("total_id"),
        F.max("enrolled_at").alias("latest_enrolled_at"),
    )
    .orderBy("category")
)
