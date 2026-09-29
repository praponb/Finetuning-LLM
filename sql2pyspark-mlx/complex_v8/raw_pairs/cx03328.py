from pyspark.sql import functions as F

prepared_enrollments = (
    df_enrollments
    .filter((F.col("term").isin("2025T2", "2025T1")) & (F.col("course_id").isNotNull()))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.date_format(F.col("enrolled_at"), "MMM yyyy").alias("enrolled_at_label"),
        F.when(F.col("grade_status").isin("INCOMPLETE", "FAIL"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
    )
)

enriched = (
    prepared_enrollments.alias("s")
    .join(df_courses.alias("c"), (F.col("c.id") == F.col("s.course_id")) & (F.upper(F.trim(F.col("c.course_code"))).like("C%")), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at",
        "s.enrolled_at_label",
        "s.grade_status_group",
        F.col("c.dept").alias("course_dept"),
        F.col("c.level").alias("course_level"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term") == "2024T2", 1)).alias("enrollment_term_count"),
        F.min("score").alias("min_enrollment_score"),
    )
)

profiled = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        "e.min_enrollment_score",
    )
)

grade_status_group_stats = (
    profiled
    .groupBy("grade_status_group")
    .agg(F.avg("enrollment_term_count").alias("avg_enrollment_term_count"))
)

compared = (
    profiled.alias("s")
    .join(grade_status_group_stats.alias("g"), F.col("g.grade_status_group") == F.col("s.grade_status_group"), "inner")
    .filter(F.col("s.enrollment_term_count") > F.col("g.avg_enrollment_term_count"))
    .select(
        "s.*",
        "g.avg_enrollment_term_count",
        F.round(F.col("s.enrollment_term_count") - F.col("g.avg_enrollment_term_count"), 2).alias("enrollment_term_count_minus_avg"),
    )
)

result = (
    compared
    .select(
        "id",
        "student_id",
        "score",
        "enrolled_at",
        "enrolled_at_label",
        "avg_enrollment_term_count",
        (F.col("min_enrollment_score") + F.col("enrollment_term_count")).alias("min_enrollment_score_plus_enrollment_term_count"),
        F.coalesce(F.col("score"), F.lit(0.0)).alias("score_filled"),
    )
    .orderBy(F.col("score_filled").asc_nulls_last(), "id")
    .limit(10)
)
