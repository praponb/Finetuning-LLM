from pyspark.sql import functions as F

cleaned_enrollments = (
    df_enrollments
    .filter((F.col("enrolled_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")) & (F.col("grade_status") != "INCOMPLETE"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.length(F.trim(F.col("grade_status"))).alias("grade_status_len"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.round(F.avg("score"), 2).alias("avg_enrollment_score"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    cleaned_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        "e.avg_enrollment_score",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    summarized_rows
    .select("id", "score", "avg_enrollment_score")
    .unpivot(
        ids=["id"],
        values=["score", "avg_enrollment_score"],
        variableColumnName="measure",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
