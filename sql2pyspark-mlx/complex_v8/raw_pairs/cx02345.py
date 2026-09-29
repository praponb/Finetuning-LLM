from pyspark.sql import functions as F

prepared = (
    df_students
    .select(
        "id",
        "student_name",
        "enrolled_on",
        "is_international",
        F.when(F.col("major").isin("CS", "Math"), "group_a").when(F.col("major") == "Bio", "group_b").otherwise("other").alias("major_group"),
    )
)

labeled = (
    prepared
    .select(
        "id",
        "enrolled_on",
        "is_international",
        F.initcap(F.trim(F.col("student_name"))).alias("student_name_clean"),
        F.dayofweek(F.col("enrolled_on")).alias("enrolled_on_dayofweek"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count(F.when(F.col("term") != "2025T1", 1)).alias("enrollment_term_count"),
        F.count("*").alias("enrollment_count"),
        F.count(F.when(F.col("term").isin("2024T1", "2024T2", "2025T2"), 1)).alias("enrollment_term_count_2"),
    )
)

with_counts = (
    labeled.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_term_count"), F.lit(0)).alias("enrollment_term_count"),
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_term_count_2"), F.lit(0)).alias("enrollment_term_count_2"),
    )
)

result = (
    with_counts
    .rollup("enrolled_on_dayofweek", "enrollment_count")
    .agg(F.max("enrollment_count").alias("max_enrollment_count"))
    .orderBy(
        F.col("enrolled_on_dayofweek").asc_nulls_last(),
        F.col("enrollment_count").asc_nulls_last(),
    )
)
