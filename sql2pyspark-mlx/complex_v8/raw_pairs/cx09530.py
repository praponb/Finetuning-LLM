from pyspark.sql import functions as F

base = (
    df_students
    .filter((F.col("major").isin("Bio", "CS", "Biz")) & (F.upper(F.trim(F.col("student_name"))).like("M%")))
    .select(
        "id",
        "student_name",
        "cohort",
        "enrolled_on",
        "gpa",
        "is_international",
        F.substring(F.trim(F.col("cohort")), 1, 2).alias("cohort_prefix"),
        F.when(F.col("gpa") >= 3.2, "large").when(F.col("gpa") >= 2.1, "medium").otherwise("small").alias("gpa_tier"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(F.count("*").alias("enrollment_count"))
)

with_stats = (
    base.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

stats = (
    with_stats
    .filter(F.col("cohort_prefix").isNotNull())
    .groupBy("enrollment_count")
    .agg(
        F.sum("enrollment_count").alias("total_enrollment_count"),
        F.countDistinct("gpa_tier").alias("distinct_gpa_tiers"),
        F.count("*").alias("cnt"),
        F.min("enrollment_count").alias("min_enrollment_count"),
        F.max("enrolled_on").alias("last_enrolled_on"),
    )
)

with_activity = (
    stats
    .crossJoin(stats.agg(F.avg("total_enrollment_count").alias("overall_avg_total_enrollment_count")))
    .filter(F.col("total_enrollment_count") > F.col("overall_avg_total_enrollment_count"))
    .drop("overall_avg_total_enrollment_count")
)

result = (
    with_activity
    .filter(F.col("cnt").between(3, 10))
    .select(
        "enrollment_count",
        "distinct_gpa_tiers",
        "cnt",
        "min_enrollment_count",
        "last_enrolled_on",
        F.quarter(F.col("last_enrolled_on")).alias("last_enrolled_on_quarter"),
    )
    .orderBy("last_enrolled_on")
)
