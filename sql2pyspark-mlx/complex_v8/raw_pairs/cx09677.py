from pyspark.sql import functions as F

scoped = (
    df_enrollments
    .filter(F.col("grade_status") == "WITHDRAWN")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("term"), F.lit("#"), F.col("student_id").cast("string")).alias("term_ref"),
    )
)

tagged = (
    scoped
    .filter(F.col("score") > 38.1)
    .withColumn("grade_status_len", F.length(F.trim(F.col("grade_status"))))
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

profiled = (
    tagged.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

qualified = (
    profiled.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.col("c.dept").isin("CS", "Bio")) & (F.col("c.credits") <= 4), "left_semi")
)

result = (
    qualified
    .select(
        "id",
        "student_id",
        "course_id",
        "enrolled_at",
        "term_ref",
        "grade_status_len",
        F.length(F.trim(F.col("term_ref"))).alias("term_ref_len"),
    )
    .orderBy(F.col("enrolled_at").desc())
)
