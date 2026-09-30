from pyspark.sql import functions as F

course_side = (
    df_courses
    .filter((F.col("dept") == "Art") & (F.upper(F.trim(F.col("course_code"))).like("C%")))
    .select(F.col("id").alias("ref_id"), "credits")
)

enrollment_rows = (
    df_enrollments
    .filter((F.col("term") != "2024T1") & (F.col("student_id").isNotNull()))
    .select(
        F.col("student_id").alias("ref_id"),
        F.col("course_id").alias("credits"),
    )
)

stacked = (
    course_side
    .unionByName(enrollment_rows)
    .distinct()
)

result = (
    stacked
    .groupBy("ref_id")
    .agg(F.count("*").alias("n_rows"), F.sum("credits").alias("total_credits"))
    .orderBy("ref_id")
)
