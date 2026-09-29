from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.to_date(F.col("enrolled_at")) < F.lit("2025-02-24").cast("date")) & (F.col("grade_status").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        F.lower(F.regexp_replace(F.trim(F.col("grade_status")), " ", "_")).alias("grade_status_slug"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
    )
)

profiled = (
    prepared_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        "e.last_enrollment_enrolled_at",
    )
)

derived = (
    profiled
    .filter(F.col("grade_status").isin("INCOMPLETE", "WITHDRAWN"))
    .select(
        "*",
        F.when(F.col("enrollment_count") >= 5, "large").when(F.col("enrollment_count") >= 3, "medium").otherwise("small").alias("enrollment_count_tier"),
        F.greatest(F.col("score"), F.col("enrollment_count")).alias("greatest_score_enrollment_count"),
    )
)

result = (
    derived
    .cube("enrollment_count", "student_id", "course_id")
    .agg(
        F.grouping_id().alias("gid"),
        F.min("last_enrollment_enrolled_at").alias("first_last_enrollment_enrolled_at"),
        F.array_sort(F.collect_set("term")).alias("term_list"),
    )
    .orderBy(
        F.col("enrollment_count").asc_nulls_last(),
        F.col("student_id").asc_nulls_last(),
        F.col("course_id").asc_nulls_last(),
    )
)
