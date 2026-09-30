from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.lit("enrollment").alias("source"),
        F.col("term").alias("category"),
        "score",
    )
)

enrollment_rows = (
    df_enrollments
    .select(F.lit("enrollment_2").alias("source"), "grade_status", "score")
)

unioned = enrollment_side.union(enrollment_rows)

result = (
    unioned
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("score").alias("total_score"))
    .orderBy("source")
)
