from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("id").alias("ref_id"),
        F.col("student_id").alias("key_id"),
        F.col("score").alias("metric"),
    )
)

enrollment_side_2 = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select("course_id", "id", "score")
)

combined = (
    enrollment_side
    .union(enrollment_side_2)
    .distinct()
)

result = (
    combined
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.sum("key_id").alias("total_key_id"))
    .orderBy("ref_id")
)
