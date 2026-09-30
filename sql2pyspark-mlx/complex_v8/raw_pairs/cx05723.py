from pyspark.sql import functions as F

recent_students = (
    df_students
    .filter((F.lower(F.col("student_name")).like("%ar%")) & (F.col("major").isNotNull()))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        F.when(F.col("gpa") >= 3.2, "A").when(F.col("gpa") >= 2.5, "B").otherwise("C").alias("gpa_tier"),
    )
)

exploded_detail = (
    recent_students.alias("s")
    .join(df_enrollments.alias("e"), F.col("e.student_id") == F.col("s.id"), "inner")
    .select(
        "s.id",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.course_id").alias("enrollment_course_id"),
    )
)

enrollment_course_id_stats = (
    exploded_detail
    .groupBy("enrollment_course_id")
    .agg(F.avg("enrollment_score").alias("avg_enrollment_score"))
)

compared = (
    exploded_detail.alias("s")
    .join(enrollment_course_id_stats.alias("g"), F.col("g.enrollment_course_id") == F.col("s.enrollment_course_id"), "inner")
    .filter(F.col("s.enrollment_score") > F.col("g.avg_enrollment_score"))
    .select(
        "s.*",
        "g.avg_enrollment_score",
        F.round(F.col("s.enrollment_score") - F.col("g.avg_enrollment_score"), 2).alias("enrollment_score_minus_avg"),
    )
)

without_matches = (
    compared.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.enrollment_course_id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-31 14:00:00").cast("timestamp")), "left_anti")
)

result = (
    without_matches
    .cube("enrollment_course_id", "enrollment_grade_status")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count(F.when(F.col("enrollment_grade_status").isin("WITHDRAWN", "INCOMPLETE"), 1)).alias("n_enrollment_grade_status_in_w"),
    )
    .select(
        "enrollment_course_id",
        F.coalesce(F.col("enrollment_grade_status"), F.lit("ALL")).alias("enrollment_grade_status"),
        "grouping_level",
        "n_enrollment_grade_status_in_w",
    )
)
