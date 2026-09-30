from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter(F.lower(F.col("course_code")).like("%35%"))
    .select(
        "id",
        "course_code",
        "credits",
        "level",
    )
)

detail_rows_enrollments = (
    prepared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.enrolled_at") >= F.lit("2025-02-06 11:00:00").cast("timestamp")), "left")
    .select(
        "s.id",
        "s.level",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.score").alias("enrollment_score"),
    )
)

annotated = (
    detail_rows_enrollments
    .select(
        "id",
        "enrollment_id",
        "enrollment_enrolled_at",
        "enrollment_score",
        F.lower(F.regexp_replace(F.trim(F.col("enrollment_grade_status")), " ", "_")).alias("enrollment_grade_status_slug"),
        F.dayofweek(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_dayofweek"),
        F.coalesce(F.col("enrollment_score"), F.lit(0.0)).alias("enrollment_score_filled"),
    )
)

stats = (
    annotated
    .filter(F.col("enrollment_score_filled") <= 85.0)
    .groupBy(
        "enrollment_enrolled_at_dayofweek",
        F.year("enrollment_enrolled_at").alias("enrollment_enrolled_at_year"),
        "enrollment_id",
    )
    .agg(
        F.max("enrollment_score").alias("max_enrollment_score"),
        F.count("*").alias("row_count"),
    )
)

enriched_enrollment = (
    stats.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.id") == F.col("s.enrollment_id")) & (F.col("e.grade_status") != "PASS"), "inner")
    .select(
        "s.enrollment_enrolled_at_dayofweek",
        "s.enrollment_enrolled_at_year",
        "s.enrollment_id",
        "s.max_enrollment_score",
        "s.row_count",
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

result = (
    enriched_enrollment
    .select(
        "enrollment_enrolled_at_dayofweek",
        "enrollment_enrolled_at_year",
        "max_enrollment_score",
        "row_count",
    )
)
