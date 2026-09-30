from pyspark.sql import functions as F

prepared = (
    df_students
    .filter((F.col("gpa") > 2.5) & (F.col("major") != "CS"))
    .select(
        "id",
        "student_name",
        "cohort",
        "major",
        "enrolled_on",
        "gpa",
        "is_international",
        F.when(F.col("is_international"), "yes").otherwise("no").alias("is_international_label"),
    )
)

enrollment_stats = (
    df_enrollments
    .groupBy("student_id")
    .agg(
        F.count("*").alias("enrollment_count"),
        F.count("*").alias("enrollment_count_2"),
        F.sum("score").alias("total_enrollment_score"),
    )
)

with_stats = (
    prepared.alias("s")
    .join(enrollment_stats.alias("e"), F.col("e.student_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("e.enrollment_count"), F.lit(0)).alias("enrollment_count"),
        F.coalesce(F.col("e.enrollment_count_2"), F.lit(0)).alias("enrollment_count_2"),
        "e.total_enrollment_score",
    )
)

scored = (
    with_stats
    .select(
        "*",
        F.date_trunc("year", F.col("enrolled_on")).alias("enrolled_on_year"),
        F.month(F.col("enrolled_on")).alias("enrolled_on_month"),
        F.date_format(F.col("enrolled_on"), "MMM yyyy").alias("enrolled_on_label"),
    )
)

result = (
    scored
    .select(
        "id",
        "major",
        "enrolled_on",
        "is_international_label",
        "enrollment_count",
        "enrollment_count_2",
        "total_enrollment_score",
        "enrolled_on_month",
        F.lower(F.trim(F.col("is_international_label"))).alias("is_international_label_clean"),
    )
    .orderBy(
        F.col("is_international_label").desc(),
        F.col("enrolled_on").asc_nulls_last(),
    )
)
