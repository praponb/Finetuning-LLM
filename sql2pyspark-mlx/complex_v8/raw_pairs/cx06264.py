from pyspark.sql import functions as F

enrollment_side = df_enrollments.select(F.col("term").alias("label"), F.col("score").alias("value"))

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") >= 30.0) & (F.month(F.col("enrolled_at")).isin(1, 3)))
    .select(F.col("grade_status").alias("label"), F.col("score").alias("value"))
)

result = (
    enrollment_side
    .unionByName(enrollment_rows)
    .distinct()
)
