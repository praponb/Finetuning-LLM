from pyspark.sql import functions as F

base = (
    df_enrollments
    .filter(F.col("student_id").isNotNull())
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.when(F.col("grade_status").isin("PASS", "FAIL"), "group_a").when(F.col("grade_status") == "WITHDRAWN", "group_b").otherwise("other").alias("grade_status_group"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        F.coalesce(F.col("grade_status"), F.lit("UNKNOWN")).alias("grade_status_or_unknown"),
    )
)

filtered = (
    base.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.col("e.student_id") >= 8) & (F.col("e.grade_status") != "WITHDRAWN"), "left_anti")
)

combined_details_course = (
    filtered.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "left")
    .select("s.*", F.col("c.level").alias("course_level"))
)

grade_status_group_stats = (
    combined_details_course
    .groupBy("grade_status_group")
    .agg(F.avg("enrolled_at_month").alias("avg_enrolled_at_month"))
)

compared = (
    combined_details_course.alias("s")
    .join(grade_status_group_stats.alias("g"), F.col("g.grade_status_group") == F.col("s.grade_status_group"), "inner")
    .filter(F.col("s.enrolled_at_month") > F.col("g.avg_enrolled_at_month"))
    .select(
        "s.*",
        "g.avg_enrolled_at_month",
        F.round(F.col("s.enrolled_at_month") - F.col("g.avg_enrolled_at_month"), 2).alias("enrolled_at_month_minus_avg"),
    )
)

derived = (
    compared
    .filter(F.col("grade_status_group").isin("group_a", "other"))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_group",
        "enrolled_at_month",
        "grade_status_or_unknown",
        "course_level",
        "avg_enrolled_at_month",
        "enrolled_at_month_minus_avg",
        F.when(F.col("enrolled_at_month") >= 9, "A").when(F.col("enrolled_at_month") >= 4, "B").otherwise("C").alias("enrolled_at_month_tier"),
        F.length(F.trim(F.col("grade_status_group"))).alias("grade_status_group_len"),
    )
)

result = (
    derived
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "grade_status_group",
        "enrolled_at_month",
        "grade_status_or_unknown",
        "course_level",
        "avg_enrolled_at_month",
        "enrolled_at_month_minus_avg",
        F.round(F.col("enrolled_at_month") / F.nullif(F.col("avg_enrolled_at_month"), F.lit(0)), 3).alias("enrolled_at_month_per_avg_enrolled_at_month"),
        F.concat(F.col("course_level"), F.lit("#"), F.col("enrolled_at_month").cast("string")).alias("course_level_ref"),
    )
    .orderBy("id")
)
