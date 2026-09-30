from pyspark.sql import functions as F

scoped_enrollments = (
    df_enrollments
    .filter((F.col("course_id") <= 6) & (F.col("score").between(32.9, 85.2)))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "grade_status",
        "enrolled_at",
        F.when(F.col("score") >= 76.8, "large").when(F.col("score") >= 38.1, "medium").otherwise("small").alias("score_tier"),
        F.date_trunc("year", F.col("enrolled_at")).alias("enrolled_at_year"),
    )
)

stats = (
    scoped_enrollments
    .groupBy("grade_status", "course_id")
    .agg(
        F.count(F.when(F.col("term").isin("2025T1", "2024T2", "2025T2"), 1)).alias("n_term_in_2025t1_2024t2_2025t2"),
        F.sum(F.when(F.col("grade_status") != "INCOMPLETE", 1).otherwise(0)).alias("n_grade_status_incomplete"),
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("n_grade_status_is_not_null"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("course_id")
    .agg(
        F.count(F.when(F.col("grade_status").isNotNull(), 1)).alias("enrollment_grade_status_count"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

with_counts = (
    stats.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.course_id") == F.col("s.course_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_grade_status_count"), F.lit(0)).alias("enrollment_grade_status_count"),
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

joined = (
    with_counts.alias("s")
    .join(df_courses.alias("c"), F.col("c.id") == F.col("s.course_id"), "inner")
    .select(
        "s.grade_status",
        "s.course_id",
        "s.n_grade_status_is_not_null",
        "s.enrollment_grade_status_count",
        "s.last_enrollment_enrolled_at",
        F.col("c.level").alias("course_level"),
    )
)

result = (
    joined
    .select(
        "grade_status",
        "course_id",
        F.col("enrollment_grade_status_count").alias("m_enrollment_grade_status_count"),
        F.col("n_grade_status_is_not_null").alias("m_n_grade_status_is_not_null"),
    )
    .unpivot(
        ids=["grade_status", "course_id"],
        values=["m_enrollment_grade_status_count", "m_n_grade_status_is_not_null"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
