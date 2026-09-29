from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(F.col("course_id").alias("qty"), F.col("student_id").alias("entity_id"))
)

enrollment_side_2 = (
    df_enrollments
    .select(F.col("course_id").alias("qty"), F.col("id").alias("entity_id"))
)

enrollment_side_3 = (
    df_enrollments
    .filter((F.col("grade_status") != "FAIL") & (F.month(F.col("enrolled_at")).isin(1, 3)))
    .select(F.col("student_id").alias("qty"), F.col("course_id").alias("entity_id"))
)

result = (
    enrollment_side
    .unionByName(enrollment_side_2)
    .unionByName(enrollment_side_3)
    .orderBy("qty", "entity_id")
)
