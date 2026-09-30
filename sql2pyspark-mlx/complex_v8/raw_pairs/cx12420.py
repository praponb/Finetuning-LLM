from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        F.col("id").alias("key_id"),
        "grade_status",
        F.col("student_id").alias("ref_id"),
    )
)

enrollment_rows = (
    df_enrollments
    .select(
        F.col("id").alias("key_id"),
        F.col("term").alias("grade_status"),
        F.col("student_id").alias("ref_id"),
    )
)

course_side = (
    df_courses
    .filter((F.col("level") == "advanced") & (F.col("dept") == "Bio"))
    .select(
        F.col("credits").alias("key_id"),
        F.col("dept").alias("grade_status"),
        F.col("id").alias("ref_id"),
    )
)

combined = (
    enrollment_side
    .unionByName(enrollment_rows)
    .unionByName(course_side)
)

result = (
    combined
    .groupBy("grade_status")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("grade_status")
)
