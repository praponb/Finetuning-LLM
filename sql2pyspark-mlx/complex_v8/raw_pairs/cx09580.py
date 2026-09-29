from pyspark.sql import functions as F

valid = (
    df_enrollments
    .filter(F.col("score") <= 78.8)
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.initcap(F.trim(F.col("grade_status"))).alias("grade_status_clean"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.round(F.avg("score"), 2).alias("avg_enrollment_score"))
)

profiled = (
    valid.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.avg_enrollment_score")
)

result = (
    profiled
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_clean",
        "avg_enrollment_score",
        F.lower(F.trim(F.col("grade_status_clean"))).alias("grade_status_clean_clean"),
        F.when(F.col("score") >= 69.6, "A").when(F.col("score") >= 23.7, "B").otherwise("C").alias("score_tier"),
    )
    .orderBy(F.col("id").desc(), "grade_status_clean")
)
