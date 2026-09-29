from pyspark.sql import functions as F

valid_students = (
    df_students
    .filter(F.col("major").isNotNull())
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("cohort").isin("C2021", "C2022"), "group_a").when(F.col("cohort") == "C2024", "group_b").otherwise("other").alias("cohort_group"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.max("enrolled_at").alias("last_enrollment_enrolled_at"),
        F.count("*").alias("enrollment_count"),
    )
)

summarized_rows = (
    valid_students.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "e.last_enrollment_enrolled_at",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
    )
)

result = (
    summarized_rows
    .groupingSets(
        [["is_international", "enrollment_count", "cohort"], ["is_international"], ["enrollment_count"]],
        "is_international", "enrollment_count", "cohort",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.count("*").alias("row_count"),
        F.sum("enrollment_count").alias("total_enrollment_count"),
        F.round(100 * F.count(F.when(F.col("major").isin("Math", "Art"), 1)) / F.count("*"), 1).alias("pct_major_in_math_art"),
    )
    .orderBy(
        F.col("is_international").asc_nulls_last(),
        F.col("enrollment_count").asc_nulls_last(),
        F.col("cohort").asc_nulls_last(),
    )
)
