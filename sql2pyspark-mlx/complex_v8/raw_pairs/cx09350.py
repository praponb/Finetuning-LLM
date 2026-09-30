from pyspark.sql import functions as F

course_rows = (
    df_courses
    .filter((F.col("credits") < 3) & (F.lower(F.col("course_code")).like("%cs%")))
    .select(
        F.lit("course").alias("src"),
        "dept",
        F.col("level").alias("category"),
        F.col("id").alias("entity_id"),
    )
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("score") <= 85.2) & (~F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE")))
    .select(
        F.lit("enrollment").alias("src"),
        F.col("term").alias("dept"),
        F.col("grade_status").alias("category"),
        F.col("course_id").alias("entity_id"),
    )
)

result = (
    course_rows
    .unionByName(enrollment_rows)
    .orderBy(F.col("dept").desc())
)
