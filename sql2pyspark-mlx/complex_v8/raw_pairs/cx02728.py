from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter(F.month(F.col("enrolled_at")).isin(2, 3))
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
    prepared_enrollments
    .filter((F.col("score") >= 10.8) & (F.col("term").isNotNull()))
    .select("grade_status")
)

enrollment_side = (
    df_enrollments
    .filter((F.col("score") < 87.8) & (F.col("course_id").isNotNull()))
    .select("grade_status")
)

result = (
    enrollment_rows
    .intersect(enrollment_side)
    .orderBy("grade_status")
)
