from pyspark.sql import functions as F

recent = (
    df_enrollments
    .filter(F.col("grade_status") == "PASS")
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("enrolled_at")).alias("days_since_enrolled_at"),
        F.hour(F.col("enrolled_at")).alias("enrolled_at_hour"),
    )
)

totals = (
    recent
    .filter(F.col("enrolled_at_hour") > 8)
    .groupBy(
        "course_id",
        F.to_date("enrolled_at").alias("enrolled_at_day"),
        "grade_status",
    )
    .agg(
        F.sum(F.when(F.col("term") == "2024T1", 1).otherwise(0)).alias("n_term_2024t1"),
        F.round(F.sum("days_since_enrolled_at") / F.nullif(F.sum("enrolled_at_hour"), F.lit(0)), 3).alias("days_since_enrolled_at_to_enrolled_at_hour_ratio"),
    )
)

combined_details = (
    totals.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.col("c.level").alias("course_level"),
        F.col("c.dept").alias("course_dept"),
    )
)

with_activity = (
    combined_details
    .crossJoin(combined_details.agg(F.avg("days_since_enrolled_at_to_enrolled_at_hour_ratio").alias("overall_avg_days_since_enrolled_at_to_enrolled_a")))
    .filter(F.col("days_since_enrolled_at_to_enrolled_at_hour_ratio") > F.col("overall_avg_days_since_enrolled_at_to_enrolled_a"))
    .drop("overall_avg_days_since_enrolled_at_to_enrolled_a")
)

combined_details_course = (
    with_activity.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select("s.*", F.col("c.credits").alias("course_credits"))
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.max("enrolled_at").alias("last_enrollment_enrolled_at"))
)

with_counts = (
    combined_details_course.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at")
)

joined = (
    with_counts.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select(
        "s.course_id",
        "s.enrolled_at_day",
        "s.grade_status",
        "s.days_since_enrolled_at_to_enrolled_at_hour_ratio",
        "s.course_level",
        "s.course_dept",
        "s.course_credits",
        "c.course_code",
    )
)

result = (
    joined
    .filter(F.col("course_dept").isNotNull())
    .select(
        "course_id",
        "days_since_enrolled_at_to_enrolled_at_hour_ratio",
        "course_level",
        "course_credits",
        "course_code",
        F.coalesce(F.col("course_credits"), F.lit(0)).alias("course_credits_filled"),
        F.quarter(F.col("enrolled_at_day")).alias("enrolled_at_day_quarter"),
    )
    .orderBy(F.col("enrolled_at_day_quarter").asc())
)
