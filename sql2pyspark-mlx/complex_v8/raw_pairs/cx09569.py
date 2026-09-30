from pyspark.sql import functions as F

base_enrollments = (
    df_enrollments
    .filter((F.col("student_id").isNotNull()) | (F.col("enrolled_at") >= F.lit("2025-01-07 11:00:00").cast("timestamp")))
    .select(
        "id",
        "student_id",
        "course_id",
        "score",
        "grade_status",
        "enrolled_at",
    )
)

with_parent_student = (
    base_enrollments.alias("s")
    .join(df_students.alias("st"), (F.col("st.id") == F.col("s.student_id")) & (F.col("st.gpa") <= 3.4), "inner")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.score",
        "s.grade_status",
        "s.enrolled_at",
        "st.student_name",
    )
)

grouped = (
    with_parent_student
    .groupBy(
        F.date_trunc("month", "enrolled_at").alias("enrolled_at_month"),
        "course_id",
    )
    .agg(
        F.array_sort(F.collect_set("grade_status")).alias("grade_status_list"),
        F.count(F.when(F.col("grade_status") == "PASS", 1)).alias("n_grade_status_pass"),
        F.round(100 * F.count(F.when(F.upper(F.trim(F.col("student_name"))).like("G%"), 1)) / F.count("*"), 1).alias("pct_upper_trim_student_name_like"),
        F.round(F.sum("score"), 2).alias("total_score"),
        F.sum(F.when(~F.col("grade_status").isin("WITHDRAWN", "INCOMPLETE"), 1).otherwise(0)).alias("n_grade_status_not_in_withdraw"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    grouped.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

enrollment_stats_2 = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.min("enrolled_at").alias("first_enrollment_enrolled_at"))
)

with_stats_2 = (
    with_stats.alias("s")
    .join(enrollment_stats_2.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select("s.*", "e.first_enrollment_enrolled_at")
)

with_details = (
    with_stats_2.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.enrolled_at_month",
        "s.course_id",
        "s.n_grade_status_pass",
        "s.pct_upper_trim_student_name_like",
        "s.total_score",
        "s.n_grade_status_not_in_withdraw",
        "s.enrollment_count",
        F.col("c.dept").alias("course_dept"),
        F.col("c.credits").alias("course_credits"),
        F.col("c.level").alias("course_level"),
    )
)

result = (
    with_details
    .filter(F.col("n_grade_status_not_in_withdraw") <= 8)
    .select(
        "enrolled_at_month",
        "course_id",
        "n_grade_status_pass",
        "pct_upper_trim_student_name_like",
        "total_score",
        "n_grade_status_not_in_withdraw",
        "enrollment_count",
        "course_dept",
        "course_credits",
        F.date_trunc("day", F.col("enrolled_at_month")).alias("enrolled_at_month_day"),
        F.upper(F.trim(F.col("course_level"))).alias("course_level_clean"),
    )
)
