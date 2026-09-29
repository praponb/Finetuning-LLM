from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("student_id").alias("entity_id"),
        F.col("term").alias("name"),
        F.col("score").alias("metric"),
        F.lit("enrollment").alias("source"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") <= 59.8) & (F.col("course_id").isNotNull()))
    .select(
        "student_id",
        "term",
        "score",
        F.lit("enrollment_2").alias("source"),
    )
)

result = (
    enrollment_side
    .union(enrollment_rows)
    .distinct()
    .orderBy("entity_id", "source")
)
