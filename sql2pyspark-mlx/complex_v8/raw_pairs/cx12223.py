from pyspark.sql import functions as F

filtered_students = (
    df_students
    .filter((F.col("gpa") < 3.4) | (~F.col("major").isin("Art", "Bio")))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
    )
)

enriched = (
    filtered_students
    .filter(F.col("major") != "CS")
    .select(
        "id",
        "student_name",
        "major",
        "enrolled_on",
        "gpa",
        F.coalesce(F.col("major"), F.lit("UNKNOWN")).alias("major_or_unknown"),
        F.add_months(F.col("enrolled_on"), 12).alias("enrolled_on_plus_12m"),
        F.lower(F.regexp_replace(F.trim(F.col("student_name")), " ", "_")).alias("student_name_slug"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.min("enrolled_at").alias("first_enrollment_enrolled_at"),
        F.min("enrolled_at").alias("first_enrollment_enrolled_at_2"),
        F.max("score").alias("max_enrollment_score"),
    )
)

with_stats = (
    enriched.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.first_enrollment_enrolled_at",
        "e.first_enrollment_enrolled_at_2",
        "e.max_enrollment_score",
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.date_format(F.col("enrolled_on_plus_12m"), "yyyy-MM").alias("enrolled_on_plus_12m_label"),
    )
)

screened = (
    scored.alias("s")
    .join(df_enrollments.alias("e"), (F.col("e.student_id") == F.col("s.id")) & (F.col("e.enrolled_at") >= F.lit("2025-01-31 08:00:00").cast("timestamp")), "left_anti")
)

totals = (
    screened
    .filter(F.col("gpa") <= 3.9)
    .groupBy(
        F.date_trunc("month", "first_enrollment_enrolled_at_2").alias("first_enrollment_enrolled_at_2_month"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum("gpa"), 2).alias("total_gpa"),
        F.max("first_enrollment_enrolled_at_2").alias("last_first_enrollment_enrolled_at_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("gpa"), 2).alias("avg_gpa"),
        F.sum(F.when(F.col("major") != "Bio", 1).otherwise(0)).alias("n_major_bio"),
    )
)

annotated = (
    totals
    .select(
        "first_enrollment_enrolled_at_2_month",
        "last_first_enrollment_enrolled_at_2",
        "distinct_ids",
        "avg_gpa",
        "n_major_bio",
        F.least(F.col("avg_gpa"), F.col("cnt")).alias("least_avg_gpa_cnt"),
        (F.floor(F.col("distinct_ids") / 2) * 2).alias("distinct_ids_bucket"),
    )
)

result = (
    annotated
    .filter(F.col("distinct_ids_bucket") < 7)
    .select(
        "first_enrollment_enrolled_at_2_month",
        "last_first_enrollment_enrolled_at_2",
        "distinct_ids",
        "avg_gpa",
        "n_major_bio",
        "distinct_ids_bucket",
    )
    .orderBy(
        F.col("last_first_enrollment_enrolled_at_2").desc(),
        "first_enrollment_enrolled_at_2_month",
    )
    .limit(5)
)
