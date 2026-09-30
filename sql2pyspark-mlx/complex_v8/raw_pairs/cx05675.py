from pyspark.sql import functions as F

prepared = (
    df_courses
    .filter(F.col("credits") >= 2)
    .select(
        "id",
        "course_code",
        "dept",
        "credits",
        "level",
        F.coalesce(F.col("credits"), F.lit(0)).alias("credits_filled"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(F.count("*").alias("enrollment_count"))
)

profiled = (
    prepared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

filtered = (
    profiled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.student_id").isNotNull()), "left_anti")
)

with_children = (
    filtered.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.id")) & (F.col("e.score") > 34.2), "left")
    .select(
        "s.id",
        "s.course_code",
        "s.credits",
        "s.credits_filled",
        "s.enrollment_count",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.score").alias("enrollment_score"),
        F.col("e.grade_status").alias("enrollment_grade_status"),
        F.col("e.student_id").alias("enrollment_student_id"),
        F.col("e.term").alias("enrollment_term"),
    )
)

qualified = (
    with_children.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.enrollment_student_id")) & (F.col("e.enrolled_at") < F.lit("2025-02-18 10:00:00").cast("timestamp")) & (F.col("e.score").between(17.4, 66.0)), "left_semi")
)

per_group = (
    qualified
    .filter(F.col("enrollment_term").isNotNull())
    .groupBy("enrollment_student_id", "enrollment_count", "credits_filled")
    .agg(
        F.sum(F.when(F.col("enrollment_term") == "2025T1", 1).otherwise(0)).alias("n_enrollment_term_2025t1"),
        F.round(100 * F.count(F.when(F.col("enrollment_term").isin("2024T2", "2024T1"), 1)) / F.count("*"), 1).alias("pct_enrollment_term_in_2024t2_20"),
        F.count("*").alias("cnt"),
        F.array_sort(F.collect_set("enrollment_grade_status")).alias("enrollment_grade_status_list"),
        F.array_sort(F.collect_set("course_code")).alias("course_code_list"),
        F.countDistinct("enrollment_term").alias("distinct_enrollment_terms"),
    )
    .filter(F.col("cnt") >= 2)
)

result = (
    per_group
    .filter(F.col("credits_filled").between(1, 3))
    .select(
        "enrollment_student_id",
        "enrollment_count",
        "credits_filled",
        "n_enrollment_term_2025t1",
        "enrollment_grade_status_list",
        "distinct_enrollment_terms",
        F.least(F.col("cnt"), F.col("credits_filled")).alias("least_cnt_credits_filled"),
        F.round(F.col("enrollment_count") / F.nullif(F.col("n_enrollment_term_2025t1"), F.lit(0)), 3).alias("enrollment_count_per_n_enrollment_term_2025t1"),
    )
    .orderBy(
        F.col("enrollment_count_per_n_enrollment_term_2025t1").desc(),
        "n_enrollment_term_2025t1",
    )
)
