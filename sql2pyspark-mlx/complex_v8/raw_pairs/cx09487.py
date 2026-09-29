from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        F.lit("enrollment").alias("origin"),
        "id",
        "enrolled_at",
        "score",
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter((F.col("grade_status").isin("INCOMPLETE", "PASS")) & (F.month(F.col("enrolled_at")).isin(2, 3)))
    .select(
        F.lit("enrollment_2").alias("origin"),
        "id",
        "enrolled_at",
        "score",
    )
)

result = (
    enrollment_side
    .union(enrollment_side_2)
    .orderBy("score")
)
