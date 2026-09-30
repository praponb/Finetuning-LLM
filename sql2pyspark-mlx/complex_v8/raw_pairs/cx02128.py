from pyspark.sql import functions as F

valid_enrollments = (
    df_enrollments
    .filter((F.col("score") >= 38.3) | (F.col("term").isin("2025T2", "2024T2")))
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "grade_status",
        "enrolled_at",
        F.concat(F.col("grade_status"), F.lit("#"), F.col("student_id").cast("string")).alias("grade_status_ref"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month"),
        (F.col("enrolled_at") + F.expr("INTERVAL 1 DAYS")).alias("enrolled_at_plus_1d"),
    )
)

with_activity = (
    valid_enrollments.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.student_id")) & (F.col("e.enrolled_at") < F.lit("2025-03-14 07:00:00").cast("timestamp")), "left_anti")
)

labeled = (
    with_activity
    .select(
        "id",
        "student_id",
        "course_id",
        "term",
        "score",
        "enrolled_at_month",
        "enrolled_at_plus_1d",
        F.dayofweek(F.col("enrolled_at")).alias("enrolled_at_dayofweek"),
        F.month(F.col("enrolled_at")).alias("enrolled_at_month_2"),
    )
)

exploded_detail_enrollments = (
    labeled.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.course_id") == F.col("s.course_id")) & (F.month(F.col("e.enrolled_at")).isin(1, 2)), "left")
    .select(
        "s.id",
        "s.student_id",
        "s.course_id",
        "s.term",
        "s.score",
        "s.enrolled_at_month",
        "s.enrolled_at_plus_1d",
        "s.enrolled_at_dayofweek",
        "s.enrolled_at_month_2",
        F.col("e.id").alias("enrollment_id"),
        F.col("e.enrolled_at").alias("enrollment_enrolled_at"),
        F.col("e.student_id").alias("enrollment_student_id"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.max("enrolled_at").alias("last_enrollment_enrolled_at_2"),
    )
)

profiled = (
    exploded_detail_enrollments.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.student_id"), "left")
    .select("s.*", "e.last_enrollment_enrolled_at", "e.last_enrollment_enrolled_at_2")
)

enriched = (
    profiled
    .filter(F.col("enrollment_id") <= 9)
    .select(
        "*",
        F.substring(F.trim(F.col("term")), 1, 3).alias("term_prefix"),
        F.upper(F.trim(F.col("term"))).alias("term_clean"),
        F.hour(F.col("enrollment_enrolled_at")).alias("enrollment_enrolled_at_hour"),
    )
)

result = (
    enriched
    .select(
        "id",
        "enrollment_id",
        F.col("enrollment_enrolled_at_hour").alias("m_enrollment_enrolled_at_hour"),
        F.col("enrolled_at_month").alias("enrolled_at_month_v"),
        F.col("enrolled_at_dayofweek").alias("m_enrolled_at_dayofweek"),
    )
    .unpivot(
        ids=["id", "enrollment_id"],
        values=["m_enrollment_enrolled_at_hour", "enrolled_at_month_v", "m_enrolled_at_dayofweek"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
