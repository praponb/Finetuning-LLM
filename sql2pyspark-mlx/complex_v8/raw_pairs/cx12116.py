from pyspark.sql import functions as F

recent_enrollments = (
    df_enrollments
    .filter(F.col("course_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.abs(F.col("score") - 39.4).alias("score_distance"),
    )
)

enriched = (
    recent_enrollments.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.grade_status",
        "s.enrolled_at",
        "c.course_code",
    )
)

derived = (
    enriched
    .select(
        "*",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
    )
)

summary = (
    derived
    .groupBy(F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.countDistinct("term").alias("distinct_terms"),
        F.countDistinct("student_id").alias("distinct_student_ids"),
    )
)

result = (
    summary
    .groupingSets(
        [["distinct_student_ids", "distinct_terms", "cnt"], ["distinct_student_ids"], []],
        "distinct_student_ids", "distinct_terms", "cnt",
    )
    .agg(
        F.grouping("distinct_student_ids").alias("is_total_distinct_student_ids"),
        F.max("enrolled_at_month").alias("last_enrolled_at_month"),
    )
    .orderBy(
        F.col("distinct_student_ids").asc_nulls_last(),
        F.col("distinct_terms").asc_nulls_last(),
        F.col("cnt").asc_nulls_last(),
    )
)
