from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter(~F.col("grade_status").isin("FAIL", "INCOMPLETE"))
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
    base
    .filter(F.col("student_id") <= 3)
    .select(
        "student_id",
        F.col("enrolled_at").alias("happened_at"),
        "score",
        F.lit("enrollment").alias("record_type"),
    )
)

enrollment_side = (
    df_enrollments
    .filter((F.col("term").isNotNull()) & (F.col("student_id") <= 9))
    .select(
        F.col("course_id").alias("student_id"),
        F.col("enrolled_at").alias("happened_at"),
        "score",
        F.lit("enrollment_2").alias("record_type"),
    )
)

result = (
    enrollment_rows
    .union(enrollment_side)
    .orderBy("score", "student_id")
)
