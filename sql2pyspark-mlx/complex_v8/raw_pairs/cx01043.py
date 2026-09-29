from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("grade_status").isin("PASS", "WITHDRAWN")) | (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.to_date(F.col("enrolled_at")).alias("enrolled_at_date"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.max("score").alias("max_enrollment_score"),
        F.count("*").alias("enrollment_count"),
    )
)

with_stats = (
    scoped_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        "e.max_enrollment_score",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

with_parent = (
    with_stats.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.dept").alias("course_dept"))
)

rollup_base = (
    with_parent
    .groupBy(F.date_format("enrolled_at", "yyyy-MM").alias("enrolled_at_ym"))
    .agg(
        F.round(F.avg("max_enrollment_score"), 2).alias("avg_max_enrollment_score"),
        F.count("*").alias("n_rows"),
        F.min("enrollment_count").alias("min_enrollment_count"),
        F.max("enrolled_at_date").alias("last_enrolled_at_date"),
        F.count(F.when(F.col("term") == "2024T2", 1)).alias("n_term_2024t2"),
        F.round(F.stddev("enrollment_count"), 2).alias("stddev_enrollment_count"),
    )
    .filter(F.col("n_rows") >= 3)
)

result = (
    rollup_base
    .select("n_term_2024t2", "n_rows")
    .distinct()
    .orderBy("n_term_2024t2")
)
