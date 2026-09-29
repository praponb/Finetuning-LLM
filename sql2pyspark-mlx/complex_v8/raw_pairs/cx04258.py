from pyspark.sql import functions as F

enrollment_side = (
    df_enrollments
    .select(
        "grade_status",
        F.col("student_id").alias("ref_id"),
        F.lit("enrollment").alias("source"),
    )
)

course_rows = (
    df_courses
    .filter(F.col("dept") == "Art")
    .select("level", "credits", F.lit("course").alias("source"))
)

unioned = enrollment_side.union(course_rows)

result = (
    unioned
    .groupBy("source")
    .agg(F.count("*").alias("n_rows"), F.sum("ref_id").alias("total_ref_id"))
    .orderBy("source")
)
