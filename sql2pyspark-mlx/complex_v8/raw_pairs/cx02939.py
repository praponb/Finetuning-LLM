from pyspark.sql import functions as F

enrollment_rows = (
    df_enrollments
    .select(
        F.col("course_id").alias("entity_id"),
        "score",
        F.lit("enrollment").alias("origin"),
    )
)

enrollment_rows_2 = (
    df_enrollments
    .select(
        F.col("course_id").alias("entity_id"),
        "score",
        F.lit("enrollment_2").alias("origin"),
    )
)

combined = enrollment_rows.unionByName(enrollment_rows_2)

result = (
    combined
    .groupBy("origin")
    .agg(
        F.count("*").alias("n_rows"),
        F.sum("entity_id").alias("total_entity_id"),
    )
    .orderBy("origin")
)
